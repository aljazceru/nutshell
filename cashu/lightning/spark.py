import asyncio
import inspect
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
    """Return a normalized identifier (payment_hash) that matches the stored mint quote checking_id."""
    try:
        details = getattr(payment, "details", None)
        if details:
            # Only log details for debugging when needed
            # logger.debug(
            #     f"Spark extract: payment.id={getattr(payment, 'id', None)} type={type(payment)} "
            #     f"details_type={type(details)} has_invoice={hasattr(details, 'invoice')} "
            #     f"has_bolt11={hasattr(details, 'bolt11_invoice')} has_hash={hasattr(details, 'payment_hash')}"
            # )

            # First priority: payment_hash (most reliable for matching)
            payment_hash = getattr(details, "payment_hash", None)
            if payment_hash:
                # logger.debug(f"Spark extract: using details.payment_hash={payment_hash}")
                return payment_hash.lower()

            # Second priority: extract hash from invoice if available
            invoice = getattr(details, "invoice", None)
            if invoice:
                try:
                    from bolt11 import decode as bolt11_decode
                    invoice_obj = bolt11_decode(invoice)
                    if invoice_obj.payment_hash:
                        # logger.debug(f"Spark extract: extracted payment_hash from invoice={invoice_obj.payment_hash}")
                        return invoice_obj.payment_hash.lower()
                except Exception:
                    pass
                # Fallback to full invoice if can't extract hash
                # logger.debug(f"Spark extract: using details.invoice={invoice[:50]}...")
                return invoice.lower()

            bolt11_details = getattr(details, "bolt11_invoice", None)
            if bolt11_details:
                bolt11 = getattr(bolt11_details, "bolt11", None)
                if bolt11:
                    try:
                        from bolt11 import decode as bolt11_decode
                        invoice_obj = bolt11_decode(bolt11)
                        if invoice_obj.payment_hash:
                            # logger.debug(f"Spark extract: extracted payment_hash from bolt11={invoice_obj.payment_hash}")
                            return invoice_obj.payment_hash.lower()
                    except Exception:
                        pass
                    # logger.debug(f"Spark extract: using bolt11_details.bolt11={bolt11[:50]}...")
                    return bolt11.lower()

        # Fallback: check payment-level payment_hash
        payment_hash = getattr(payment, "payment_hash", None)
        if payment_hash:
            # logger.debug(f"Spark extract: using payment.payment_hash={payment_hash}")
            return payment_hash.lower()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error(f"Failed to extract Spark invoice identifier: {exc}")

    return None


def _get_payment_fee_sats(payment) -> Optional[int]:
    """Return the payment fee in satoshis if available."""
    fee = None

    for attr in ("fee_sats", "fees", "fee"):
        if hasattr(payment, attr):
            fee = getattr(payment, attr)
            if fee is not None:
                break

    if fee is None:
        details = getattr(payment, "details", None)
        if details is not None and hasattr(details, "fees"):
            fee = getattr(details, "fees")

    if fee is None:
        return None

    try:
        return int(fee)
    except (TypeError, ValueError):
        try:
            return int(str(fee))
        except (TypeError, ValueError):
            return None


def _get_payment_preimage(payment) -> Optional[str]:
    """Return the payment preimage if exposed by the SDK."""
    preimage = getattr(payment, "preimage", None)
    if preimage:
        return preimage

    details = getattr(payment, "details", None)
    if details and hasattr(details, "preimage"):
        return getattr(details, "preimage") or None

    return None


# Import Spark SDK components
set_sdk_event_loop = None
_get_sdk_event_loop = None
try:
    from breez_sdk_spark import breez_sdk_spark as spark_bindings
    from breez_sdk_spark import (
        BreezSdk,
        connect,
        ConnectRequest,
        default_config,
        EventListener,
        GetInfoRequest,
        GetPaymentRequest,
        PaymentMethod,
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
        Seed,
    )
    # Event loop fix will be imported but not applied yet
    set_sdk_event_loop = None
    _get_sdk_event_loop = None
    try:
        from .spark_event_loop_fix import (
            set_sdk_event_loop as _set_sdk_event_loop,
            ensure_event_loop as _ensure_event_loop,
        )
        set_sdk_event_loop = _set_sdk_event_loop
        _get_sdk_event_loop = _ensure_event_loop
        if spark_bindings is not None:
            try:
                spark_bindings._uniffi_get_event_loop = _ensure_event_loop
                logger.debug("Patched breez_sdk_spark._uniffi_get_event_loop")
            except Exception as exc:  # pragma: no cover
                logger.warning(f"Failed to patch Spark SDK event loop getter: {exc}")
    except ImportError:
        _get_sdk_event_loop = None
    # uniffi_set_event_loop is not available in newer versions
    spark_uniffi_set_event_loop = None
    common_uniffi_set_event_loop = None
except ImportError as e:
    # Create dummy classes for when SDK is not available
    spark_bindings = None
    BreezSdk = None
    EventListener = None
    PaymentMethod = None
    SparkPaymentStatus = None
    spark_uniffi_set_event_loop = None
    common_uniffi_set_event_loop = None
    logger.warning(f"Breez SDK Spark not available - SparkBackend will not function: {e}")


def _get_payment_amount_sats(payment) -> Optional[int]:
    """Return the payment amount in satoshis if available."""
    amount = getattr(payment, "amount", None)
    if amount is None:
        return None
    try:
        return int(amount)
    except (TypeError, ValueError):
        try:
            return int(str(amount))
        except (TypeError, ValueError):
            return None


def _is_lightning_payment(payment) -> bool:
    """Check whether the payment method represents a Lightning payment."""
    method = getattr(payment, "method", None)
    lightning_method = getattr(PaymentMethod, "LIGHTNING", None)
    if lightning_method is None or method is None:
        return True  # fall back to permissive behavior if enum missing
    return method == lightning_method


if EventListener is not None:
    class SparkEventListener(EventListener):
        """Event listener for Spark SDK payment notifications"""

        def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
            super().__init__()
            self.queue = queue
            self.loop = loop

        async def on_event(self, event: SdkEvent) -> None:
            """Handle SDK events in a thread-safe manner with robust error handling"""
            try:
                # Debug log ALL events to understand what types of events we get
                logger.info(f"Spark SDK event received: {event.__class__.__name__}, hasPayment={hasattr(event, 'payment')}, event={event}")

                # Check if this is a payment event we care about and extract the invoice id
                if hasattr(event, "payment"):
                    payment = event.payment
                    status = getattr(payment, "status", None)
                    payment_type = getattr(payment, "payment_type", None)
                    receive_type = getattr(PaymentType, "RECEIVE", None)

                    # Debug log all payment events to understand what we're getting
                    logger.info(f"Spark payment event: status={status}, type={payment_type}, payment={payment}")

                    # Only consider completed/settled payments
                    if status and hasattr(SparkPaymentStatus, 'COMPLETED') and status != SparkPaymentStatus.COMPLETED:
                        # Check if it's a different completion status
                        if not (hasattr(SparkPaymentStatus, 'SETTLED') and status == SparkPaymentStatus.SETTLED):
                            logger.debug(
                                f"Spark event {event.__class__.__name__} ignored (status {status})"
                            )
                            return

                    # Require RECEIVE payment type if enum exists
                    if receive_type and payment_type and payment_type != receive_type:
                        logger.debug(
                            f"Spark event {event.__class__.__name__} ignored (type {payment_type})"
                        )
                        return

                    if not _is_lightning_payment(payment):
                        logger.debug(
                            f"Spark event {event.__class__.__name__} ignored (non-lightning method {getattr(payment, 'method', None)})"
                        )
                        return

                    amount_sats = _get_payment_amount_sats(payment)
                    if amount_sats is not None and amount_sats <= 0:
                        logger.debug(
                            f"Spark event {event.__class__.__name__} ignored (non-positive amount {amount_sats})"
                        )
                        return

                    checking_id = _extract_invoice_checking_id(payment)
                    logger.debug(
                        f"Spark event {event.__class__.__name__} payment_type={getattr(payment, 'payment_type', None)} "
                        f"status={getattr(payment, 'status', None)} payment_id={getattr(payment, 'id', None)} "
                        f"raw_payment={payment!r} extracted_id={checking_id}"
                    )
                    if not checking_id:
                        logger.debug(f"Spark event {event.__class__.__name__} ignored (no checking id)")
                        return

                    # More robust thread-safe event handling
                    self._safe_put_event(checking_id)

            except Exception as e:
                logger.error(f"Error handling Spark event: {e}")
                import traceback
                logger.debug(f"Event handler traceback: {traceback.format_exc()}")

        def _safe_put_event(self, checking_id: str) -> None:
            """Safely put an event into the queue from any thread context"""
            try:
                target_loop = self.loop
                if (target_loop is None or target_loop.is_closed()) and callable(_get_sdk_event_loop):
                    alt_loop = _get_sdk_event_loop()
                    if alt_loop and not alt_loop.is_closed():
                        target_loop = alt_loop

                if target_loop is None:
                    logger.warning("Spark event listener has no target loop; dropping event")
                    return

                if target_loop.is_closed():
                    logger.warning("Spark event listener target loop is closed; dropping event")
                    return

                # Use call_soon_threadsafe for more reliable thread-safe event handling
                def queue_put():
                    try:
                        self.queue.put_nowait(checking_id)
                        logger.info(f"Spark event successfully queued: {checking_id}")
                    except asyncio.QueueFull:
                        logger.warning(f"Spark event queue full, dropping event: {checking_id}")
                    except Exception as e:
                        logger.error(f"Failed to put event in queue: {e}")

                target_loop.call_soon_threadsafe(queue_put)

            except Exception as exc:
                logger.warning(f"Failed to queue Spark event (expected from callback thread): {exc}")
                # Fallback: try the original approach
                try:
                    if self.loop and not self.loop.is_closed():
                        future = asyncio.run_coroutine_threadsafe(
                            self.queue.put(checking_id),
                            self.loop,
                        )
                        logger.info(f"Spark event fallback queued: {checking_id}")
                except Exception as fallback_exc:
                    logger.error(f"Both event queueing methods failed: {fallback_exc}")
else:
    class SparkEventListener:
        """Dummy event listener when Spark SDK is not available"""

        def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
            self.queue = queue
            self.loop = loop

        async def on_event(self, event) -> None:
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
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
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

        event_loop = asyncio.get_running_loop()
        # Store the event loop for SDK callbacks
        applied = False
        if callable(set_sdk_event_loop):
            try:
                set_sdk_event_loop(event_loop)
                applied = True
                logger.debug("Registered Spark SDK event loop via Python fix")
            except Exception as exc:  # pragma: no cover - defensive log
                logger.warning(f"Failed to set Spark SDK event loop via python fix: {exc}")

        for setter in (spark_uniffi_set_event_loop, common_uniffi_set_event_loop):
            if callable(setter):
                try:
                    setter(event_loop)
                    applied = True
                    logger.debug(f"Registered Spark SDK event loop via {setter.__name__}")
                except Exception as exc:  # pragma: no cover - defensive log
                    logger.warning(f"Failed to register event loop with Spark SDK: {exc}")

        if not applied:
            logger.warning(
                "Spark SDK event loop could not be registered; callbacks may fail. "
                "Ensure the shim in cashu/lightning/spark_event_loop_fix.py is available."
            )

        # ConnectRequest requires a Seed object (mnemonic or entropy based)
        seed = Seed.MNEMONIC(mnemonic=mnemonic, passphrase=None)
        self.sdk = await asyncio.wait_for(
            connect(
                request=ConnectRequest(
                    config=config,
                    seed=seed,
                    storage_dir=storage_dir
                )
            ),
            timeout=connection_timeout
        )

        # Set up event listener for payment notifications
        self.event_queue = asyncio.Queue()
        self._event_loop = event_loop
        self.listener = SparkEventListener(self.event_queue, self._event_loop)
        self.listener_id = await _await_if_needed(
            self.sdk.add_event_listener(listener=self.listener)
        )
        logger.debug(f"Spark SDK initialized successfully on {network_str} network")

        # Clear mnemonic from memory
        mnemonic = None
        del mnemonic

    async def cleanup(self) -> None:
        """Proper resource cleanup"""
        try:
            if hasattr(self, 'listener_id') and self.sdk:
                if self.listener_id:
                    await _await_if_needed(
                        self.sdk.remove_event_listener(id=self.listener_id)
                    )

            if self.sdk:
                await _await_if_needed(self.sdk.disconnect())

        except Exception as e:
            logger.error(f"Cleanup error: {e}")
        finally:
            self.sdk = None
            self.listener = None
            self.event_queue = None
            self.listener_id = None
            self._event_loop = None
            self._initialized = False

    async def _check_connectivity(self) -> bool:
        """Quick connectivity check"""
        try:
            if not self.sdk:
                return False
            await asyncio.wait_for(
                self.sdk.get_info(request=GetInfoRequest(ensure_synced=None)),
                timeout=5.0
            )
            return True
        except Exception:
            return False

    async def status(self) -> StatusResponse:
        try:
            await self._ensure_initialized()
            info = await self.sdk.get_info(request=GetInfoRequest(ensure_synced=None))
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

            # Extract payment_hash from the invoice for consistent matching
            from bolt11 import decode as bolt11_decode
            try:
                invoice_obj = bolt11_decode(response.payment_request)
                payment_hash = invoice_obj.payment_hash
                logger.debug(
                    f"Spark create_invoice amount={amount} payment_hash={payment_hash} invoice={response.payment_request[:50]}..."
                )
            except Exception as e:
                logger.error(f"Failed to extract payment_hash from invoice: {e}")
                # Fallback to using full invoice as checking_id
                payment_hash = response.payment_request.lower()

            checking_id_to_store = payment_hash.lower() if payment_hash else response.payment_request.lower()
            logger.info(f"Spark storing checking_id: {checking_id_to_store[:20]}... (hash: {bool(payment_hash)})")

            return InvoiceResponse(
                ok=True,
                checking_id=checking_id_to_store,
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

            fee_sats = _get_payment_fee_sats(payment)
            preimage = _get_payment_preimage(payment)
            checking_id = (
                quote.checking_id
                or _extract_invoice_checking_id(payment)
                or getattr(payment, "payment_hash", None)
                or getattr(payment, "id", None)
            )

            return PaymentResponse(
                result=result,
                checking_id=checking_id,
                fee=Amount(Unit.sat, fee_sats) if fee_sats is not None else None,
                preimage=preimage
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
            # We need to get all RECEIVE payments and find the one with this payment_request
            from .base import PaymentResult

            receive_type = getattr(PaymentType, "RECEIVE", None)
            type_filter = [receive_type] if receive_type else None
            if type_filter:
                list_request = ListPaymentsRequest(type_filter=type_filter)
            else:
                list_request = ListPaymentsRequest()
            list_response = await self.sdk.list_payments(request=list_request)
            normalized_checking_id = checking_id.lower()

            for payment in list_response.payments:
                payment_type = getattr(payment, "payment_type", None)
                if receive_type and payment_type and payment_type != receive_type:
                    continue

                if not _is_lightning_payment(payment):
                    continue

                amount_sats = _get_payment_amount_sats(payment)
                if amount_sats is not None and amount_sats <= 0:
                    logger.debug(
                        "Spark get_invoice_status skipping zero-amount receive payment id=%s",
                        getattr(payment, "id", None),
                    )
                    continue

                payment_checking_id = _extract_invoice_checking_id(payment)
                if payment_checking_id and payment_checking_id == normalized_checking_id:
                    # Found our payment - return its status
                    logger.debug(
                        f"Spark payment found: target={normalized_checking_id} status={getattr(payment, 'status', None)}"
                    )
                    result = self._map_payment_status(payment)
                    fee_sats = _get_payment_fee_sats(payment)
                    preimage = _get_payment_preimage(payment)
                    return PaymentStatus(
                        result=result,
                        fee=Amount(Unit.sat, fee_sats) if fee_sats is not None else None,
                        preimage=preimage
                    )

            # If not found in payments list, invoice is still pending
            logger.debug(f"Spark payment not found for checking_id: {normalized_checking_id[:20]}...")
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

            # Melt checking_id represents the outgoing invoice/payment hash.
            # Query SEND payments (or all if enum missing) so we can match outgoing attempts.
            send_type = getattr(PaymentType, "SEND", None)
            type_filter = [send_type] if send_type else None
            if type_filter:
                list_request = ListPaymentsRequest(type_filter=type_filter)
            else:
                list_request = ListPaymentsRequest()

            response = await self.sdk.list_payments(request=list_request)

            # Find the payment with matching invoice
            target_payment = None
            checking_id_lower = checking_id.lower()

            for payment in response.payments:
                payment_type = getattr(payment, "payment_type", None)
                if send_type and payment_type and payment_type != send_type:
                    continue

                if not _is_lightning_payment(payment):
                    continue

                amount_sats = _get_payment_amount_sats(payment)
                if amount_sats is not None and amount_sats <= 0:
                    logger.debug(
                        "Spark get_payment_status skipping zero-amount send payment id=%s",
                        getattr(payment, "id", None),
                    )
                    continue

                # Check if this payment's invoice hash matches our checking_id
                invoice_id = _extract_invoice_checking_id(payment)
                if invoice_id and invoice_id.lower() == checking_id_lower:
                    target_payment = payment
                    logger.debug(f"Found matching payment for invoice {checking_id[:20]}...")
                    break

            if not target_payment:
                logger.debug(f"No payment found for checking_id {checking_id[:20]}...")
                return PaymentStatus(
                    result=PaymentResult.PENDING,
                    error_message="Payment not found yet"
                )

            # Map Spark payment status to PaymentResult
            result = self._map_payment_status(target_payment)
            fee_sats = _get_payment_fee_sats(target_payment)
            preimage = _get_payment_preimage(target_payment)

            return PaymentStatus(
                result=result,
                fee=Amount(Unit.sat, fee_sats) if fee_sats is not None else None,
                preimage=preimage
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
                logger.debug(f"Spark paid_invoices_stream emitting checking_id={payment_id}")
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
async def _await_if_needed(value):
    """Await value if it is awaitable; otherwise return it directly."""
    if inspect.isawaitable(value):
        return await value
    return value
