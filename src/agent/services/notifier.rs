//! Notifier
//!
//! The notifier is responsible for notifying subscribers who have registered
//! for price sched updates.

use {
    crate::agent::state::Prices,
    std::sync::Arc,
    tracing::instrument,
};

#[instrument(skip(state))]
pub async fn notifier<S>(state: Arc<S>)
where
    S: Prices,
{
    let mut interval = tokio::time::interval(state.notify_interval_duration());
    let mut exit = crate::agent::EXIT.subscribe();
    loop {
        Prices::drop_closed_subscriptions(&*state).await;
        tokio::select! {
            _ = exit.changed() => {
                tracing::info!("Shutdown signal received.");
                return;
            }
            _ = interval.tick() => {
                if let Err(err) = state.send_notify_price_sched().await {
                    tracing::error!(err = ?err, "Notifier: failed to send notify price sched.");
                }
            }
        }
    }
}
