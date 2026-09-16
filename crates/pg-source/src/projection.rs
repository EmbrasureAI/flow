//! Explicit column projection at the wire boundary, before decoding values.
use crate::{Error, Relation, Result, SourceEvent, Tuple};
use std::collections::{HashMap, HashSet};

fn offsets(relation: &Relation, selected: &[String]) -> Result<Vec<usize>> {
    if selected.is_empty() || selected.iter().collect::<HashSet<_>>().len() != selected.len() {
        return Err(Error::Config(
            "column selection must be nonempty and unique",
        ));
    }
    let result = selected
        .iter()
        .map(|name| {
            relation
                .columns
                .iter()
                .position(|column| column.name == *name)
                .ok_or(Error::Config(
                    "selected source column disappeared; resynchronization is required",
                ))
        })
        .collect::<Result<Vec<_>>>()?;
    if !result.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(Error::Config(
            "selected columns must retain source column order",
        ));
    }
    Ok(result)
}

pub fn project_relation(relation: &Relation, selected: &[String]) -> Result<Relation> {
    let indices = offsets(relation, selected)?;
    let mut projected = relation.clone();
    projected.columns = indices
        .into_iter()
        .map(|index| relation.columns[index].clone())
        .collect();
    Ok(projected)
}

#[derive(Default)]
pub struct EventProjector {
    selected: HashMap<u32, Vec<String>>,
    wire: HashMap<u32, (usize, Vec<usize>)>,
}
impl EventProjector {
    pub fn new(selections: impl IntoIterator<Item = (u32, Vec<String>)>) -> Result<Self> {
        let mut projector = Self::default();
        for (id, names) in selections {
            if names.is_empty()
                || names.iter().collect::<HashSet<_>>().len() != names.len()
                || projector.selected.insert(id, names).is_some()
            {
                return Err(Error::Config("invalid or duplicate table column selection"));
            }
        }
        Ok(projector)
    }
    fn tuple(&self, id: u32, row: &mut Tuple) -> Result<()> {
        if !self.selected.contains_key(&id) {
            return Ok(());
        }
        let (width, indices) = self
            .wire
            .get(&id)
            .ok_or(Error::Protocol("projected tuple before relation"))?;
        if row.len() != *width {
            return Err(Error::Protocol("tuple column count differs from relation"));
        }
        // Bytes clones only reference-count buffers; excluded cells are never decoded.
        *row = indices.iter().map(|index| row[*index].clone()).collect();
        Ok(())
    }
    /// Keep only available explicitly selected fields when schema drift has
    /// removed/reordered a selection. Excluded cells never enter quarantine.
    pub fn quarantine_relation(&mut self, relation: &Relation) -> Relation {
        let Some(selected) = self.selected.get(&relation.id) else {
            return relation.clone();
        };
        let indices: Vec<_> = relation
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| selected.contains(&column.name).then_some(index))
            .collect();
        self.wire
            .insert(relation.id, (relation.columns.len(), indices.clone()));
        let mut projected = relation.clone();
        projected.columns = indices
            .iter()
            .map(|index| relation.columns[*index].clone())
            .collect();
        projected
    }

    pub fn project(&mut self, mut event: SourceEvent) -> Result<SourceEvent> {
        match &mut event {
            SourceEvent::Relation(relation) => {
                if let Some(selected) = self.selected.get(&relation.id) {
                    self.wire.insert(
                        relation.id,
                        (relation.columns.len(), offsets(relation, selected)?),
                    );
                    *relation = project_relation(relation, selected)?;
                }
            }
            SourceEvent::Insert { relation, row, .. } => self.tuple(*relation, row)?,
            SourceEvent::Update {
                relation, old, row, ..
            } => {
                if let Some(old) = old {
                    self.tuple(*relation, old)?;
                }
                self.tuple(*relation, row)?;
            }
            SourceEvent::Delete { relation, old, .. } => self.tuple(*relation, old)?,
            _ => {}
        }
        Ok(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cell, Column};
    fn relation(names: &[&str]) -> Relation {
        Relation {
            id: 1,
            namespace: "public".into(),
            name: "rows".into(),
            replica_identity: b'f',
            columns: names
                .iter()
                .map(|name| Column {
                    name: (*name).into(),
                    type_oid: 25,
                    type_modifier: -1,
                    identity: true,
                })
                .collect(),
        }
    }
    #[test]
    fn quarantine_after_selected_column_removal_never_keeps_excluded_cells() {
        let mut projector =
            EventProjector::new([(1, vec!["id".into(), "removed".into()])]).unwrap();
        let wire = relation(&["id", "secret"]);
        assert!(
            projector
                .project(SourceEvent::Relation(wire.clone()))
                .is_err()
        );
        let projected = projector.quarantine_relation(&wire);
        assert_eq!(projected.columns.len(), 1);
        let event = projector
            .project(SourceEvent::Insert {
                xid: 1,
                subxid: 1,
                relation: 1,
                row: vec![Cell::Text("1".into()), Cell::Text("do-not-retain".into())],
            })
            .unwrap();
        assert!(
            matches!(event, SourceEvent::Insert { row, .. } if row == vec![Cell::Text("1".into())])
        );
    }

    #[test]
    fn excluded_toast_and_changed_wire_offsets_do_not_change_selected_values() {
        let mut projector = EventProjector::new([(1, vec!["id".into(), "value".into()])]).unwrap();
        projector
            .project(SourceEvent::Relation(relation(&["ignored", "id", "value"])))
            .unwrap();
        let row = vec![
            Cell::UnchangedToast,
            Cell::Text("1".into()),
            Cell::Text("hello".into()),
        ];
        let event = projector
            .project(SourceEvent::Update {
                xid: 1,
                subxid: 1,
                relation: 1,
                old: Some(row.clone()),
                old_is_key: false,
                row,
            })
            .unwrap();
        if let SourceEvent::Update { row, old, .. } = event {
            assert_eq!(
                row,
                vec![Cell::Text("1".into()), Cell::Text("hello".into())]
            );
            assert_eq!(old.unwrap(), row);
        } else {
            panic!("expected update");
        }
        projector
            .project(SourceEvent::Relation(relation(&["id", "value"])))
            .unwrap();
        assert!(
            projector
                .project(SourceEvent::Insert {
                    xid: 1,
                    subxid: 1,
                    relation: 1,
                    row: vec![Cell::Null]
                })
                .is_err()
        );
        assert!(
            projector
                .project(SourceEvent::Relation(relation(&["id", "renamed"])))
                .is_err()
        );
    }
}
