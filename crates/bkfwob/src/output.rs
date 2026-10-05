//! Output in the toolchain's four text formats: `table` (default), `csv`, `md`, `jsonl`.
//!
//! The table, csv and md forms show the columns a person reads; jsonl is the whole record as
//! serialized, which is what a script should parse.

use std::io::Write;

use anyhow::Result;
use bkfwob_core::clock::EXCHANGE_TZ;
use bkfwob_core::types::{Account, Fill, Order, Position, Quote};
use jiff::Timestamp;
use jiff::tz::TimeZone;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Format {
    #[default]
    Table,
    Csv,
    Md,
    Jsonl,
}

impl Format {
    pub fn parse(token: &str) -> Option<Self> {
        Some(match token {
            "table" => Self::Table,
            "csv" => Self::Csv,
            "md" => Self::Md,
            "jsonl" => Self::Jsonl,
            _ => return None,
        })
    }
}

/// A rendered table: display columns plus the full JSON of each row.
pub struct Rows {
    columns: Vec<&'static str>,
    /// Right-align numeric cells; off for field/value records, where it scatters the values.
    align_numbers: bool,
    cells: Vec<Vec<String>>,
    json: Vec<Value>,
}

impl Rows {
    pub fn new(columns: &[&'static str]) -> Self {
        Self {
            columns: columns.to_vec(),
            align_numbers: true,
            cells: Vec::new(),
            json: Vec::new(),
        }
    }

    pub fn push(&mut self, cells: Vec<String>, json: impl Serialize) {
        debug_assert_eq!(cells.len(), self.columns.len());
        self.cells.push(cells);
        self.json
            .push(serde_json::to_value(json).unwrap_or(Value::Null));
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    pub fn print(&self, format: Format, out: &mut impl Write) -> Result<()> {
        match format {
            Format::Jsonl => {
                for row in &self.json {
                    writeln!(out, "{}", serde_json::to_string(row)?)?;
                }
            }
            Format::Csv => {
                writeln!(out, "{}", self.columns.join(","))?;
                for row in &self.cells {
                    let line: Vec<String> = row.iter().map(|c| csv_cell(c)).collect();
                    writeln!(out, "{}", line.join(","))?;
                }
            }
            Format::Md => {
                writeln!(out, "| {} |", self.columns.join(" | "))?;
                let rule: Vec<&str> = self.columns.iter().map(|_| "---").collect();
                writeln!(out, "| {} |", rule.join(" | "))?;
                for row in &self.cells {
                    let cells: Vec<String> = row.iter().map(|c| c.replace('|', "\\|")).collect();
                    writeln!(out, "| {} |", cells.join(" | "))?;
                }
            }
            Format::Table => {
                let mut widths: Vec<usize> = self.columns.iter().map(|c| c.len()).collect();
                for row in &self.cells {
                    for (w, c) in widths.iter_mut().zip(row) {
                        *w = (*w).max(c.chars().count());
                    }
                }
                let line = |cells: &[String]| {
                    cells
                        .iter()
                        .zip(&widths)
                        .map(|(c, w)| {
                            if self.align_numbers && looks_numeric(c) {
                                format!("{c:>w$}")
                            } else {
                                format!("{c:<w$}")
                            }
                        })
                        .collect::<Vec<_>>()
                        .join("  ")
                        .trim_end()
                        .to_string()
                };
                let header: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
                writeln!(out, "{}", line(&header))?;
                for row in &self.cells {
                    writeln!(out, "{}", line(row))?;
                }
            }
        }
        Ok(())
    }
}

fn csv_cell(c: &str) -> String {
    if c.contains([',', '"', '\n']) {
        format!("\"{}\"", c.replace('"', "\"\""))
    } else {
        c.to_string()
    }
}

fn looks_numeric(c: &str) -> bool {
    !c.is_empty() && c.parse::<f64>().is_ok()
}

/// A single record as two columns, `field` and `value`.
pub fn record(pairs: Vec<(&'static str, String)>, json: impl Serialize) -> Rows {
    let value = serde_json::to_value(json).unwrap_or(Value::Null);
    let mut rows = Rows::new(&["field", "value"]);
    rows.align_numbers = false;
    for (field, v) in pairs {
        rows.cells.push(vec![field.to_string(), v]);
    }
    rows.json = vec![value];
    rows
}

/// Shortest exact form: `10`, `0.5`, `185.25`.
pub fn num(v: f64) -> String {
    if v == v.trunc() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

pub fn money(v: f64) -> String {
    format!("{v:.2}")
}

pub fn opt(v: Option<f64>) -> String {
    v.map(num).unwrap_or_default()
}

/// Exchange-local wall time.
pub fn time(t: Timestamp) -> String {
    let tz = TimeZone::get(EXCHANGE_TZ).unwrap_or(TimeZone::UTC);
    t.to_zoned(tz).strftime("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn account(a: &Account) -> Rows {
    record(
        vec![
            ("account", a.id.clone()),
            ("mode", a.mode.to_string()),
            ("currency", a.currency.clone()),
            ("cash", money(a.cash)),
            ("buying_power", money(a.buying_power)),
            ("equity", money(a.equity)),
        ],
        a,
    )
}

pub fn positions(list: &[Position]) -> Rows {
    let mut rows = Rows::new(&["symbol", "qty", "avg_cost", "cost_basis"]);
    for p in list {
        rows.push(
            vec![
                p.instrument.symbol.clone(),
                num(p.qty),
                money(p.avg_cost),
                money(p.qty * p.avg_cost),
            ],
            p,
        );
    }
    rows
}

pub const ORDER_COLUMNS: &[&str] = &[
    "id",
    "symbol",
    "side",
    "qty",
    "kind",
    "tif",
    "session",
    "status",
    "filled",
    "avg_price",
    "tag",
    "updated",
];

pub fn order_cells(o: &Order) -> Vec<String> {
    vec![
        o.client_id.to_string(),
        o.spec.instrument.symbol.clone(),
        o.spec.side.to_string(),
        o.spec.qty.to_string(),
        o.spec.kind.to_string(),
        o.spec.tif.to_string(),
        o.spec.session.to_string(),
        o.status.to_string(),
        num(o.filled_qty),
        opt(o.avg_fill_price),
        o.spec.tag.clone().unwrap_or_default(),
        time(o.updated_at),
    ]
}

pub fn orders<'a>(list: impl IntoIterator<Item = &'a Order>) -> Rows {
    let mut rows = Rows::new(ORDER_COLUMNS);
    for o in list {
        rows.push(order_cells(o), o);
    }
    rows
}

pub fn fills(list: &[Fill]) -> Rows {
    let mut rows = Rows::new(&[
        "time",
        "symbol",
        "side",
        "qty",
        "price",
        "commission",
        "order",
        "exec_id",
    ]);
    for f in list {
        rows.push(
            vec![
                time(f.time),
                f.instrument.symbol.clone(),
                f.side.to_string(),
                num(f.qty),
                num(f.price),
                opt(f.commission),
                f.client_id.to_string(),
                f.exec_id.clone(),
            ],
            f,
        );
    }
    rows
}

pub fn quotes(list: &[Quote]) -> Rows {
    let mut rows = Rows::new(&["symbol", "bid", "ask", "last", "time"]);
    for q in list {
        rows.push(
            vec![
                q.instrument.symbol.clone(),
                opt(q.bid),
                opt(q.ask),
                opt(q.last),
                q.time.map(time).unwrap_or_default(),
            ],
            q,
        );
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Rows {
        let mut rows = Rows::new(&["symbol", "qty"]);
        rows.push(
            vec!["AAPL".into(), "10".into()],
            serde_json::json!({"symbol": "AAPL", "qty": 10}),
        );
        rows.push(
            vec!["BRK.B".into(), "2.5".into()],
            serde_json::json!({"symbol": "BRK.B", "qty": 2.5}),
        );
        rows
    }

    fn render(format: Format) -> String {
        let mut out = Vec::new();
        sample().print(format, &mut out).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn every_format() {
        assert_eq!(
            render(Format::Table),
            "symbol  qty\nAAPL     10\nBRK.B   2.5\n"
        );
        assert_eq!(render(Format::Csv), "symbol,qty\nAAPL,10\nBRK.B,2.5\n");
        assert_eq!(
            render(Format::Md),
            "| symbol | qty |\n| --- | --- |\n| AAPL | 10 |\n| BRK.B | 2.5 |\n"
        );
        let jsonl = render(Format::Jsonl);
        let first: Value = serde_json::from_str(jsonl.lines().next().unwrap()).unwrap();
        assert_eq!(first["qty"], 10);
    }

    #[test]
    fn numbers_print_without_noise() {
        assert_eq!(num(10.0), "10");
        assert_eq!(num(0.5), "0.5");
        assert_eq!(money(1.005), "1.00");
        assert_eq!(csv_cell("a,b"), "\"a,b\"");
    }
}
