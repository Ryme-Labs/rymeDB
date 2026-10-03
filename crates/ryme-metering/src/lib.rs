use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageEvent {
    pub tenant: String,
    pub database: String,
    pub metric: Metric,
    pub quantity: u64,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    ReadUnit,
    WriteUnit,
    StoredByte,
    EgressByte,
    RealtimeMessage,
    ConnectionMinute,
}

impl UsageEvent {
    pub fn new(
        tenant: String,
        database: String,
        metric: Metric,
        quantity: u64,
        idempotency_key: String,
    ) -> Self {
        Self { tenant, database, metric, quantity, idempotency_key }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MeterKey {
    pub tenant: String,
    pub database: String,
    pub metric: Metric,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MeterTotal {
    pub quantity: u64,
    pub events: u64,
}

#[derive(Debug, Default)]
pub struct MeterRegistry {
    seen: HashSet<String>,
    totals: HashMap<MeterKey, MeterTotal>,
}

impl MeterRegistry {
    pub fn new() -> Self {
        Self { seen: HashSet::new(), totals: HashMap::new() }
    }

    pub fn ingest(&mut self, event: UsageEvent) -> bool {
        if event.idempotency_key.is_empty() {
            self.add(event);
            return true;
        }
        if !self.seen.insert(event.idempotency_key.clone()) {
            return false;
        }
        self.add(event);
        true
    }

    pub fn ingest_all(&mut self, events: Vec<UsageEvent>) -> usize {
        let mut accepted = 0;
        for event in events {
            if self.ingest(event) {
                accepted += 1;
            }
        }
        accepted
    }

    pub fn total_for(&self, tenant: &str, database: &str, metric: Metric) -> MeterTotal {
        self.totals
            .get(&MeterKey { tenant: tenant.to_string(), database: database.to_string(), metric })
            .cloned()
            .unwrap_or_default()
    }

    pub fn snapshot(&self) -> Vec<(MeterKey, MeterTotal)> {
        let mut out: Vec<(MeterKey, MeterTotal)> =
            self.totals.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        out.sort_by(|a, b| {
            (&a.0.tenant, &a.0.database, metric_order(&a.0.metric)).cmp(&(
                &b.0.tenant,
                &b.0.database,
                metric_order(&b.0.metric),
            ))
        });
        out
    }

    pub fn tenant_bytes(&self, tenant: &str) -> u64 {
        self.totals
            .iter()
            .filter(|(k, _)| {
                k.tenant == tenant
                    && (k.metric == Metric::StoredByte || k.metric == Metric::EgressByte)
            })
            .map(|(_, v)| v.quantity)
            .sum()
    }

    pub fn len(&self) -> usize {
        self.totals.len()
    }

    pub fn is_empty(&self) -> bool {
        self.totals.is_empty()
    }

    fn add(&mut self, event: UsageEvent) {
        let key = MeterKey { tenant: event.tenant, database: event.database, metric: event.metric };
        let entry = self.totals.entry(key).or_default();
        entry.quantity = entry.quantity.saturating_add(event.quantity);
        entry.events = entry.events.saturating_add(1);
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TenantBill {
    pub tenant: String,
    pub read_units: u64,
    pub write_units: u64,
    pub stored_bytes: u64,
    pub egress_bytes: u64,
    pub realtime_messages: u64,
    pub connection_minutes: u64,
}

impl MeterRegistry {
    pub fn billing_summary(&self) -> Vec<TenantBill> {
        let mut bills: HashMap<String, TenantBill> = HashMap::new();
        for (key, total) in &self.totals {
            let bill = bills.entry(key.tenant.clone()).or_insert_with(|| TenantBill {
                tenant: key.tenant.clone(),
                ..TenantBill::default()
            });
            match key.metric {
                Metric::ReadUnit => {
                    bill.read_units = bill.read_units.saturating_add(total.quantity)
                }
                Metric::WriteUnit => {
                    bill.write_units = bill.write_units.saturating_add(total.quantity)
                }
                Metric::StoredByte => {
                    bill.stored_bytes = bill.stored_bytes.saturating_add(total.quantity)
                }
                Metric::EgressByte => {
                    bill.egress_bytes = bill.egress_bytes.saturating_add(total.quantity)
                }
                Metric::RealtimeMessage => {
                    bill.realtime_messages = bill.realtime_messages.saturating_add(total.quantity)
                }
                Metric::ConnectionMinute => {
                    bill.connection_minutes = bill.connection_minutes.saturating_add(total.quantity)
                }
            }
        }
        let mut out: Vec<TenantBill> = bills.into_values().collect();
        out.sort_by(|a, b| a.tenant.cmp(&b.tenant));
        out
    }

    pub fn invoice(&self, tenant: &str, prices: &PriceTable) -> Invoice {
        let mut lines: Vec<InvoiceLine> = Vec::new();
        let mut total_micros = 0u64;
        for bill in self.billing_summary() {
            if bill.tenant != tenant {
                continue;
            }
            let items = [
                ("read_unit", bill.read_units, prices.read_unit_micros),
                ("write_unit", bill.write_units, prices.write_unit_micros),
                ("stored_byte", bill.stored_bytes, prices.stored_byte_micros),
                ("egress_byte", bill.egress_bytes, prices.egress_byte_micros),
                ("realtime_message", bill.realtime_messages, prices.realtime_message_micros),
                ("connection_minute", bill.connection_minutes, prices.connection_minute_micros),
            ];
            for (name, quantity, unit_micros) in items {
                if quantity == 0 {
                    continue;
                }
                let amount = quantity.saturating_mul(unit_micros);
                total_micros = total_micros.saturating_add(amount);
                lines.push(InvoiceLine {
                    metric: name.to_string(),
                    quantity,
                    unit_micros,
                    amount_micros: amount,
                });
            }
        }
        lines.sort_by(|a, b| a.metric.cmp(&b.metric));
        Invoice { tenant: tenant.to_string(), lines, total_micros, currency: String::from("USD") }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceTable {
    pub read_unit_micros: u64,
    pub write_unit_micros: u64,
    pub stored_byte_micros: u64,
    pub egress_byte_micros: u64,
    pub realtime_message_micros: u64,
    pub connection_minute_micros: u64,
}

impl Default for PriceTable {
    fn default() -> Self {
        Self {
            read_unit_micros: 1,
            write_unit_micros: 5,
            stored_byte_micros: 0,
            egress_byte_micros: 1,
            realtime_message_micros: 1,
            connection_minute_micros: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvoiceLine {
    pub metric: String,
    pub quantity: u64,
    pub unit_micros: u64,
    pub amount_micros: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invoice {
    pub tenant: String,
    pub lines: Vec<InvoiceLine>,
    pub total_micros: u64,
    pub currency: String,
}

fn metric_order(metric: &Metric) -> u8 {
    match metric {
        Metric::ReadUnit => 0,
        Metric::WriteUnit => 1,
        Metric::StoredByte => 2,
        Metric::EgressByte => 3,
        Metric::RealtimeMessage => 4,
        Metric::ConnectionMinute => 5,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(key: &str, metric: Metric, quantity: u64) -> UsageEvent {
        UsageEvent::new(String::from("t"), String::from("d"), metric, quantity, key.to_string())
    }

    #[test]
    fn dedupes_retries() {
        let mut registry = MeterRegistry::new();
        assert!(registry.ingest(event("a", Metric::WriteUnit, 2)));
        assert!(!registry.ingest(event("a", Metric::WriteUnit, 2)));
        let total = registry.total_for("t", "d", Metric::WriteUnit);
        assert_eq!(total.quantity, 2);
        assert_eq!(total.events, 1);
    }

    #[test]
    fn aggregates_metrics() {
        let mut registry = MeterRegistry::new();
        registry.ingest_all(vec![
            event("a", Metric::ReadUnit, 3),
            event("b", Metric::ReadUnit, 4),
            event("c", Metric::StoredByte, 9),
        ]);
        assert_eq!(registry.total_for("t", "d", Metric::ReadUnit).quantity, 7);
        assert_eq!(registry.tenant_bytes("t"), 9);
    }

    #[test]
    fn billing_groups_by_tenant() {
        let mut registry = MeterRegistry::new();
        registry.ingest_all(vec![
            event("a", Metric::ReadUnit, 3),
            event("b", Metric::WriteUnit, 5),
            UsageEvent::new(
                String::from("other"),
                String::from("d"),
                Metric::ReadUnit,
                2,
                String::from("c"),
            ),
        ]);
        let bills = registry.billing_summary();
        assert_eq!(bills.len(), 2);
        let main = bills.iter().find(|b| b.tenant == "t").unwrap();
        assert_eq!(main.read_units, 3);
        assert_eq!(main.write_units, 5);
    }

    #[test]
    fn invoice_totals_line_items() {
        let mut registry = MeterRegistry::new();
        registry
            .ingest_all(vec![event("a", Metric::ReadUnit, 3), event("b", Metric::WriteUnit, 5)]);
        let invoice = registry.invoice("t", &PriceTable::default());
        assert_eq!(invoice.currency, "USD");
        assert_eq!(invoice.lines.len(), 2);
        assert_eq!(invoice.total_micros, 3 + 5 * 5);
        let empty = registry.invoice("ghost", &PriceTable::default());
        assert!(empty.lines.is_empty());
        assert_eq!(empty.total_micros, 0);
    }
}
