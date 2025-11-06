import asyncio
import math
from typing import AsyncGenerator, Optional

from bolt11 import decode
from loguru import logger

from ..core.base import Amount, MeltQuote, Unit
from ..core.helpers import fee_reserve
from ..core.models import PostMeltQuoteRequest
from ..core.settings import settings
from .base import (
    InvoiceResponse,
    LightningBackend,
    PaymentQuoteResponse,
    PaymentResponse,
    PaymentResult,
    PaymentStatus,
    StatusResponse,
)


def _extract_invoice_checking_id(payment) -> Optional[str]:
    """Return a normalized identifier that matches the stored mint quote checking_id."""
    try:
        details = getattr(payment, "details", None)
        if details:
            logger.debug(
                "Spark extract: payment.id=%s type=%s details_type=%s has_invoice=%s has_bolt11=%s has_hash=%s",
                getattr(payment, "id", None),
                type(payment),
                type(details),
                hasattr(details, "invoice"),
                hasattr(details, "bolt11_invoice"),
                hasattr(details, "payment_hash"),
            )
            invoice = getattr(details, "invoice", None)
            if invoice:
                logger.debug("Spark extract: using details.invoice=%s", invoice)
                return invoice.lower()

            bolt11_details = getattr(details, "bolt11_invoice", None)
            if bolt11_details:
                bolt11 = getattr(bolt11_details, "bolt11", None)
                if bolt11:
                    logger.debug("Spark extract: using bolt11_details.bolt11=%s", bolt11)
                    return bolt11.lower()

            payment_hash = getattr(details, "payment_hash", None)
            if payment_hash:
                logger.debug("Spark extract: using details.payment_hash=%s", payment_hash)
                return payment_hash.lower()

        payment_hash = getattr(payment, "payment_hash", None)
        if payment_hash:
            logger.debug("Spark extract: using payment.payment_hash=%s", payment_hash)
            return payment_hash.lower()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error(f"Failed to extract Spark invoice identifier: {exc}")

    return None


# Import Spark SDK components
try:
    from breez_sdk_spark import (
        BreezSdk,
        connect,
        ConnectRequest,
        default_config,
        EventListener,
        GetInfoRequest,
        GetPaymentRequest,
        ListPaymentsRequest,
        Network,
        PaymentStatus as SparkPaymentStatus,
        PaymentType,
        ReceivePaymentMethod,
        ReceivePaymentRequest,
        PrepareSendPaymentRequest,
        SdkEvent,
        SendPaymentRequest,
        SendPaymentOptions,
    )
except ImportError:
    # Create dummy classes for when SDK is not available
    BreezSdk = None
    EventListener = None
    SparkPaymentStatus = None
    logger.warning("Breez SDK Spark not available - SparkBackend will not function")


if EventListener is not None:
    class SparkEventListener(EventListener):
        """Event listener for Spark SDK payment notifications"""

        def __init__(self, queue: asyncio.Queue):
            super().__init__()
            self.queue = queue

        def on_event(self, event: SdkEvent) -> None:
            """Handle SDK events in a thread-safe manner"""
            try:
                # Check if this is a payment event we care about and extract the invoice id
                if hasattr(event, "payment"):
                    payment = event.payment
                    status = getattr(payment, "status", None)
                    if status != SparkPaymentStatus.COMPLETED:
                        logger.debug(
                            "Spark event %s ignored (status %s)",
                            event.__class__.__name__,
                            status,
                        )
                        return

                    payment_type = getattr(payment, "payment_type", None)
                    if payment_type != PaymentType.RECEIVE:
                        logger.debug(
                            "Spark event %s ignored (payment type %s)",
                            event.__class__.__name__,
                            payment_type,
                        )
                        return

                    checking_id = _extract_invoice_checking_id(payment)
                    logger.debug(
                        "Spark event %s payment_type=%s status=%s payment_id=%s raw_payment=%r extracted_id=%s",
                        event.__class__.__name__,
                        getattr(payment, "payment_type", None),
                        getattr(payment, "status", None),
                        getattr(payment, "id", None),
                        payment,
                        checking_id,
                    )
                    if not checking_id:
                        logger.debug("Spark event %s ignored (no checking id)", event.__class__.__name__)
                        return

                    try:
                        loop = asyncio.get_running_loop()
                        # Thread-safe queue put with payment hash (checking_id)
                        asyncio.run_coroutine_threadsafe(
                            self.queue.put(checking_id),
                            loop,
                        )
                    except RuntimeError:
                        logger.warning("No running event loop found for Spark event")
            except Exception as e:
                logger.error(f"Error handling Spark event: {e}")
else:
    class SparkEventListener:
        """Dummy event listener when Spark SDK is not available"""

        def __init__(self, queue: asyncio.Queue):
            self.queue = queue

        def on_event(self, event) -> None:
            """Dummy event handler"""
            logger.warning("SparkEventListener called but Spark SDK not available")


class SparkBackend(LightningBackend):
    """Breez Spark SDK Lightning backend implementation"""

    supported_units = {Unit.sat, Unit.msat}
    supports_mpp = False
    supports_incoming_payment_stream = True
    supports_description = True
    unit = Unit.sat

    def __init__(self, unit: Unit = Unit.sat, **kwargs):
        if BreezSdk is None:
            raise Exception("Breez SDK not available - install breez-sdk")

        self.assert_unit_supported(unit)
        self.unit = unit
        self.sdk: Optional[BreezSdk] = None
        self.event_queue: Optional[asyncio.Queue] = None
        self.listener: Optional[SparkEventListener] = None
        self.listener_id: Optional[str] = None
        self._initialized = False
        self._initialization_lock = asyncio.Lock()
        self._connection_retry_count = 0
        self._max_retries = getattr(settings, 'mint_spark_retry_attempts', 3)
        self._retry_delay = 5.0

        # Validate required settings
        if not settings.mint_spark_api_key:
            raise Exception("MINT_SPARK_API_KEY not set")
        if not settings.mint_spark_mnemonic:
            raise Exception("MINT_SPARK_MNEMONIC not set")

    async def __aenter__(self):
        """Async context manager entry"""
        await self._ensure_initialized()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.cleanup()

    async def _ensure_initialized(self) -> None:
        """Lazy initialization with proper error handling"""
        if self._initialized and self.sdk:
            return

        async with self._initialization_lock:
            if self._initialized and self.sdk:
                return

            try:
                await self._initialize_sdk_with_retry()
                self._initialized = True
            except Exception as e:
                logger.error(f"SDK initialization failed: {e}")
                raise

    async def _initialize_sdk_with_retry(self) -> None:
        """Initialize SDK with exponential backoff retry"""
        for attempt in range(self._max_retries):
            try:
                await self._initialize_sdk()
                self._connection_retry_count = 0
                return
            except Exception as e:
                self._connection_retry_count += 1
                if attempt == self._max_retries - 1:
                    raise

                delay = self._retry_delay * (2 ** attempt)
                logger.warning(
                    f"SDK init attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {delay}s"
                )
                await asyncio.sleep(delay)

    async def _initialize_sdk(self) -> None:
        """Initialize the Spark SDK connection"""
        mnemonic = settings.mint_spark_mnemonic

        # Determine network
        network_str = getattr(settings, 'mint_spark_network', 'mainnet').lower()
        network = Network.MAINNET if network_str == 'mainnet' else Network.TESTNET

        config = default_config(network=network)
        config.api_key = settings.mint_spark_api_key

        storage_dir = getattr(settings, 'mint_spark_storage_dir', 'data/spark')
        connection_timeout = getattr(settings, 'mint_spark_connection_timeout', 30)

        # ConnectRequest takes mnemonic directly, not a Seed object
        self.sdk = await asyncio.wait_for(
            connect(
                request=ConnectRequest(
                    config=config,
                    mnemonic=mnemonic,
                    storage_dir=storage_dir
                )
            ),
            timeout=connection_timeout
        )

        # Set up event listener for payment notifications
        self.event_queue = asyncio.Queue()
        self.listener = SparkEventListener(self.event_queue)
        # add_event_listener is not async, it returns a string ID directly
        self.listener_id = self.sdk.add_event_listener(listener=self.listener)
        logger.debug(f"Spark SDK initialized successfully on {network_str} network")

        # Clear mnemonic from memory
        mnemonic = None
        del mnemonic

    async def cleanup(self) -> None:
        """Proper resource cleanup"""
        try:
            if hasattr(self, 'listener_id') and self.sdk:
                # remove_event_listener is not async
                self.sdk.remove_event_listener(id=self.listener_id)

            if self.sdk:
                # disconnect is not async
                self.sdk.disconnect()

        except Exception as e:
            logger.error(f"Cleanup error: {e}")
        finally:
            self.sdk = None
            self.listener = None
            self.event_queue = None
            self._initialized = False

    async def _check_connectivity(self) -> bool:
        """Quick connectivity check"""
        try:
            if not self.sdk:
                return False
            await asyncio.wait_for(
                self.sdk.get_info(request=GetInfoRequest()),
                timeout=5.0
            )
            return True
        except Exception:
            return False

    async def status(self) -> StatusResponse:
        try:
            await self._ensure_initialized()
            info = await self.sdk.get_info(request=GetInfoRequest())
            return StatusResponse(
                balance=Amount(Unit.sat, info.balance_sats),
                error_message=None
            )
        except Exception as e:
            logger.error(f"Spark status error: {e}")
            return StatusResponse(
                error_message=f"Failed to connect to Spark SDK: {e}",
                balance=Amount(self.unit, 0)
            )

    async def create_invoice(
        self,
        amount: Amount,
        memo: Optional[str] = None,
        description_hash: Optional[bytes] = None,
        unhashed_description: Optional[bytes] = None,
    ) -> InvoiceResponse:
        self.assert_unit_supported(amount.unit)

        try:
            await self._ensure_initialized()

            payment_method = ReceivePaymentMethod.BOLT11_INVOICE(
                description=memo or "",
                amount_sats=amount.to(Unit.sat).amount
            )
            request = ReceivePaymentRequest(payment_method=payment_method)
            response = await self.sdk.receive_payment(request=request)
            logger.debug(
                "Spark create_invoice amount=%s response.payment_request=%s",
                amount,
                response.payment_request,
            )

            return InvoiceResponse(
                ok=True,
                checking_id=response.payment_request.lower(),
                payment_request=response.payment_request
            )
        except Exception as e:
            logger.error(f"Spark create_invoice error for amount {amount}: {e}")
            return InvoiceResponse(ok=False, error_message=f"Invoice creation failed: {e}")

    async def pay_invoice(
        self, quote: MeltQuote, fee_limit_msat: int
    ) -> PaymentResponse:
        try:
            await self._ensure_initialized()

            # Prepare the payment
            prepare_request = PrepareSendPaymentRequest(
                payment_request=quote.request,
                amount=None  # Use invoice amount
            )
            prepare_response = await self.sdk.prepare_send_payment(request=prepare_request)

            # Send the payment
            options = SendPaymentOptions.BOLT11_INVOICE(
                prefer_spark=False,
                completion_timeout_secs=30
            )
            send_request = SendPaymentRequest(
                prepare_response=prepare_response,
                options=options
            )
            send_response = await self.sdk.send_payment(request=send_request)

            payment = send_response.payment
            logger.debug(
                "Spark pay_invoice quote=%s result_payment_id=%s status=%s type=%s raw_payment=%r",
                quote.quote,
                getattr(payment, "id", None),
                getattr(payment, "status", None),
                getattr(payment, "payment_type", None),
                payment,
            )

            # Map Spark payment status to PaymentResult
            result = self._map_payment_status(payment)

            return PaymentResponse(
                result=result,
                checking_id=payment.id,
                fee=Amount(Unit.sat, payment.fee_sats) if hasattr(payment, 'fee_sats') and payment.fee_sats else None,
                preimage=payment.preimage if hasattr(payment, 'preimage') else None
            )
        except Exception as e:
            logger.error(f"Spark pay_invoice error for quote {quote.quote}: {e}")
            return PaymentResponse(
                result=PaymentResult.FAILED,
                error_message=f"Payment failed: {e}"
            )

    async def get_invoice_status(self, checking_id: str) -> PaymentStatus:
        try:
            await self._ensure_initialized()

            # For Spark SDK, checking_id is the Lightning invoice/payment_request
            # We need to get all payments and find the one with this payment_request
            from .base import PaymentResult

            # List all recent payments to find our invoice
            list_request = ListPaymentsRequest()
            list_response = await self.sdk.list_payments(request=list_request)
            normalized_checking_id = checking_id.lower()

            for payment in list_response.payments:
                payment_checking_id = _extract_invoice_checking_id(payment)
                logger.debug(
                    "Spark get_invoice_status candidate id=%s target=%s status=%s payment_id=%s raw_payment=%r",
                    payment_checking_id,
                    normalized_checking_id,
                    getattr(payment, "status", None),
                    getattr(payment, "id", None),
                    payment,
                )
                if payment_checking_id and payment_checking_id == normalized_checking_id:
                    # Found our payment - return its status
                    result = self._map_payment_status(payment)
                    return PaymentStatus(
                        result=result,
                        fee=Amount(Unit.sat, payment.fee_sats) if hasattr(payment, 'fee_sats') and payment.fee_sats else None,
                        preimage=payment.preimage if hasattr(payment, 'preimage') else None
                    )

            # If not found in payments list, invoice is still pending
            return PaymentStatus(
                result=PaymentResult.PENDING,
                error_message=None
            )

        except Exception as e:
            logger.error(f"Get invoice status error: {e}")
            return PaymentStatus(
                result=PaymentResult.UNKNOWN,
                error_message=str(e)
            )

    async def get_payment_status(self, checking_id: str) -> PaymentStatus:
        try:
            await self._ensure_initialized()

            # Get payment by payment ID
            get_request = GetPaymentRequest(payment_id=checking_id)
            response = await self.sdk.get_payment(request=get_request)
            payment = response.payment

            # Map Spark payment status to PaymentResult
            result = self._map_payment_status(payment)

            return PaymentStatus(
                result=result,
                fee=Amount(Unit.sat, payment.fee_sats) if hasattr(payment, 'fee_sats') and payment.fee_sats else None,
                preimage=payment.preimage if hasattr(payment, 'preimage') else None
            )
        except Exception as e:
            logger.error(f"Get payment status error: {e}")
            return PaymentStatus(
                result=PaymentResult.UNKNOWN,
                error_message=str(e)
            )

    def _map_payment_status(self, payment) -> PaymentResult:
        """Map Spark SDK payment status to PaymentResult enum."""
        if not hasattr(payment, 'status'):
            return PaymentResult.UNKNOWN

        # Use official PaymentStatus enum for more reliable mapping
        try:
            if payment.status == SparkPaymentStatus.COMPLETED:
                return PaymentResult.SETTLED
            elif payment.status == SparkPaymentStatus.FAILED:
                return PaymentResult.FAILED
            elif payment.status == SparkPaymentStatus.PENDING:
                return PaymentResult.PENDING
            else:
                # Fallback to string comparison for any new status values
                status_str = str(payment.status).lower()
                if 'complete' in status_str or 'settled' in status_str or 'succeeded' in status_str:
                    return PaymentResult.SETTLED
                elif 'failed' in status_str or 'cancelled' in status_str or 'expired' in status_str:
                    return PaymentResult.FAILED
                elif 'pending' in status_str or 'processing' in status_str:
                    return PaymentResult.PENDING
                else:
                    return PaymentResult.UNKNOWN
        except (AttributeError, TypeError):
            # Fallback to string-based mapping if enum comparison fails
            status_str = str(payment.status).lower()
            if 'complete' in status_str or 'settled' in status_str or 'succeeded' in status_str:
                return PaymentResult.SETTLED
            elif 'failed' in status_str or 'cancelled' in status_str or 'expired' in status_str:
                return PaymentResult.FAILED
            elif 'pending' in status_str or 'processing' in status_str:
                return PaymentResult.PENDING
            else:
                return PaymentResult.UNKNOWN

    async def get_payment_quote(
        self, melt_quote: PostMeltQuoteRequest
    ) -> PaymentQuoteResponse:
        invoice_obj = decode(melt_quote.request)
        assert invoice_obj.amount_msat, "invoice has no amount."
        amount_msat = int(invoice_obj.amount_msat)

        # Use standard fee calculation for now
        # TODO: Use Spark SDK's fee estimation when available
        fees_msat = fee_reserve(amount_msat)
        fees = Amount(unit=Unit.msat, amount=fees_msat)
        amount = Amount(unit=Unit.msat, amount=amount_msat)

        return PaymentQuoteResponse(
            checking_id=invoice_obj.payment_hash,
            fee=fees.to(self.unit, round="up"),
            amount=amount.to(self.unit, round="up"),
        )

    async def paid_invoices_stream(self) -> AsyncGenerator[str, None]:
        """Stream of paid invoice notifications with resilience"""
        await self._ensure_initialized()

        retry_delay = settings.mint_retry_exponential_backoff_base_delay
        max_retry_delay = settings.mint_retry_exponential_backoff_max_delay

        while True:
            try:
                # Set timeout to prevent infinite blocking
                payment_id = await asyncio.wait_for(
                    self.event_queue.get(),
                    timeout=30.0
                )
                logger.debug("Spark paid_invoices_stream emitting checking_id=%s", payment_id)
                yield payment_id

                # Reset retry delay on success
                retry_delay = settings.mint_retry_exponential_backoff_base_delay

            except asyncio.TimeoutError:
                # Periodic connectivity check
                if not await self._check_connectivity():
                    logger.warning("Spark connectivity lost, attempting reconnection")
                    self._initialized = False
                    await self._ensure_initialized()
                else:
                    logger.debug("Spark paid_invoices_stream heartbeat (no events)")
                continue

            except Exception as e:
                logger.error(f"Spark payment stream error: {e}")
                await asyncio.sleep(retry_delay)

                # Exponential backoff
                retry_delay = max(
                    settings.mint_retry_exponential_backoff_base_delay,
                    min(retry_delay * 2, max_retry_delay)
                )

                # Attempt recovery
                if not self._initialized:
                    await self._ensure_initialized()

    async def health_check(self) -> bool:
        """Perform comprehensive health check"""
        try:
            await self._ensure_initialized()
            return await self._check_connectivity()
        except Exception as e:
            logger.warning(f"Spark health check failed: {e}")
            return False
