import asyncio
from typing import List

from loguru import logger

from ..core.base import MintQuoteState, Method, Unit
from ..core.settings import settings
from ..lightning.base import LightningBackend
from .protocols import SupportsBackends, SupportsDb, SupportsEvents


class LedgerTasks(SupportsDb, SupportsBackends, SupportsEvents):
    async def dispatch_listeners(self) -> List[asyncio.Task]:
        tasks = []
        for method, unitbackends in self.backends.items():
            for unit, backend in unitbackends.items():
                logger.debug(
                    f"Dispatching backend invoice listener for {method} {unit} {backend.__class__.__name__}"
                )
                tasks.append(asyncio.create_task(self.invoice_listener(backend)))
        return tasks

    async def invoice_listener(self, backend: LightningBackend) -> None:
        if backend.supports_incoming_payment_stream:
            retry_delay = settings.mint_retry_exponential_backoff_base_delay
            max_retry_delay = settings.mint_retry_exponential_backoff_max_delay
            
            while True:
                try:
                    # Reset retry delay on successful connection to backend stream
                    retry_delay = settings.mint_retry_exponential_backoff_base_delay
                    async for checking_id in backend.paid_invoices_stream():
                        await self.invoice_callback_dispatcher(checking_id)
                except Exception as e:
                    logger.error(f"Error in invoice listener: {e}")
                    logger.info(f"Restarting invoice listener in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    
                    # Exponential backoff
                    retry_delay = min(retry_delay * 2, max_retry_delay)

    async def invoice_callback_dispatcher(self, checking_id: str) -> None:
        logger.debug(f"Invoice callback dispatcher: {checking_id}")
        async with self.db.get_connection(
            lock_table="mint_quotes",
            lock_select_statement=f"checking_id='{checking_id}'",
            lock_timeout=5,
        ) as conn:
            quote = await self.crud.get_mint_quote(
                checking_id=checking_id, db=self.db, conn=conn
            )
            if not quote:
                logger.error(f"Quote not found for {checking_id}")
                return

            logger.trace(
                f"Invoice callback dispatcher: quote {quote} trying to set as {MintQuoteState.paid}"
            )
            # set the quote as paid
            if quote.unpaid:
                confirmed = await self._confirm_invoice_paid_with_backend(quote)
                if not confirmed:
                    logger.debug(
                        "Invoice callback ignored for %s; backend still reports %s",
                        quote.quote,
                        "pending" if quote.unpaid else quote.state.value,
                    )
                    return
                quote.state = MintQuoteState.paid
                await self.crud.update_mint_quote(quote=quote, db=self.db, conn=conn)
                logger.trace(
                    f"Quote {quote.quote} with {MintQuoteState.unpaid} set as {quote.state.value}"
                )

        await self.events.submit(quote)

    async def _confirm_invoice_paid_with_backend(self, quote) -> bool:
        """Ensure backend agrees invoice is settled before updating DB."""
        try:
            method = Method[quote.method]
        except KeyError:
            logger.error(f"Unknown payment method on quote {quote.quote}: {quote.method}")
            return False

        try:
            unit = Unit[quote.unit]
        except KeyError:
            logger.error(f"Unknown unit on quote {quote.quote}: {quote.unit}")
            return False

        if not quote.checking_id:
            logger.error(f"Quote {quote.quote} missing checking_id; cannot verify payment")
            return False

        method_backends = self.backends.get(method)
        if not method_backends:
            logger.error(f"No backend registered for method {method}")
            return False

        backend = method_backends.get(unit)
        if not backend:
            logger.error(f"No backend registered for method {method} unit {unit}")
            return False

        try:
            status = await backend.get_invoice_status(quote.checking_id)
        except Exception as exc:
            logger.error(f"Backend verification failed for quote {quote.quote}: {exc}")
            return False

        if not status.settled:
            logger.debug(
                "Backend reported %s for quote %s; deferring state change",
                status.result,
                quote.quote,
            )
            return False

        return True
