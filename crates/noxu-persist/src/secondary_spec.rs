//! Compile-time metadata for secondary indexes declared via the
//! `#[derive(SecondaryKey)]` proc-macro.
//!
//! When a user writes
//!
//! ```ignore
//! #[derive(noxu_persist::Entity, noxu_persist::SecondaryKey)]
//! struct User {
//!     #[primary_key]
//!     id: u64,
//!     #[secondary_key(name = "by_email", relate = OneToOne)]
//!     email: String,
//!     #[secondary_key(
//!         name = "by_dept",
//!         relate = ManyToOne,
//!         related_entity = "Department",
//!         on_related_entity_delete = NULLIFY,
//!     )]
//!     dept: Option<u64>,
//! }
//! ```
//!
//! the derive emits a `pub const SECONDARY_INDEXES: &'static [SecondarySpec]`
//! describing every declared index, plus a typed
//! `open_<name>_index(primary)` helper per field.  The const-table is the
//! Rust analogue of the JE `PersistKeyMetadata` reflective view.
//!
//! `SecondarySpec` is *only* metadata: it does not own an extractor closure
//! and is therefore `Copy + 'static`.  The actual `SecondaryIndex`
//! registration is performed by the per-field helper methods that the
//! derive emits, because the secondary-key type `SK` is field-specific and
//! cannot be erased into a const table.

/// Cardinality of a secondary index relationship.
///
/// Mirrors the `Relationship` enum from BDB-JE
/// (`com.sleepycat.persist.model.Relationship`):
///
/// | JE constant | Noxu variant | Meaning |
/// |---|---|---|
/// | `ONE_TO_ONE` | `OneToOne` | each entity has exactly one secondary key, and the secondary key is unique across all entities |
/// | `MANY_TO_ONE` | `ManyToOne` | each entity has exactly one secondary key, but multiple entities may share the same secondary key (the common case) |
/// | `ONE_TO_MANY` | `OneToMany` | each entity has multiple secondary keys; each secondary key is unique |
/// | `MANY_TO_MANY` | `ManyToMany` | each entity has multiple secondary keys, multiple entities may share keys |
///
/// noxu-persist's `SecondaryIndex` extractor signature is
/// `Fn(&E) -> Option<SK>` — one secondary key per entity — so only
/// `OneToOne` and `ManyToOne` are fully exercised by the engine.
/// `OneToMany` / `ManyToMany` are accepted by the derive (so the metadata
/// round-trips cleanly) but the extractor still returns a single key; users
/// that need multi-key extraction should register multiple secondary
/// indexes manually until a multi-key extractor lands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Relate {
    /// One entity ↔ one secondary key, unique across the index.
    OneToOne,
    /// Many entities → one secondary key (the common foreign-key shape).
    ManyToOne,
    /// One entity → many secondary keys, each unique.
    OneToMany,
    /// Many entities → many secondary keys.
    ManyToMany,
}

/// Action taken when an entity referenced by a foreign-key secondary index
/// is deleted from its primary `EntityStore`.
///
/// Mirrors `com.sleepycat.persist.model.DeleteAction` from BDB-JE.
///
/// DPL secondary indexes are persistent, transactional
/// `noxu-db` `SecondaryDatabase`s, but the DPL `open_secondary_index`
/// path does not yet wire this `on_related_entity_delete` attribute into
/// the lower-level `noxu-db` foreign-key cascade machinery — the field is
/// **metadata only**, recorded in `SecondarySpec` so callers can inspect
/// the user's intent. (The `noxu-db` `SecondaryConfig` foreign-key
/// support exists and can be used directly when FK enforcement is
/// required; wiring the DPL attribute into it is a future item.)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DeleteAction {
    /// Abort the deletion if any entity in the related store still
    /// references it.
    Abort,
    /// Cascade the deletion: delete every entity that references the
    /// deleted entity.
    Cascade,
    /// Set the foreign-key field to `None` in every entity that referenced
    /// the deleted entity.
    Nullify,
}

/// Compile-time metadata describing a single secondary index declared on
/// an entity via `#[secondary_key(...)]`.
///
/// Fields mirror the BDB-JE `@SecondaryKey` annotation fields one-for-one:
///
/// | JE field | Noxu field | Required |
/// |---|---|---|
/// | `name` | `name` | yes |
/// | `relate` | `relate` | yes |
/// | `relatedEntity` | `related_entity` | no |
/// | `onRelatedEntityDelete` | `on_related_entity_delete` | no (default `Abort`) |
///
/// The derive emits one `SecondarySpec` per `#[secondary_key(...)]` field,
/// collected into a `pub const SECONDARY_INDEXES: &[SecondarySpec]` on the
/// entity struct.  This is observable at runtime via
/// `Entity::secondary_specs()` (auto-implemented by the derive).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SecondarySpec {
    /// Logical name of the secondary index (the value of
    /// `#[secondary_key(name = "...")]`).  Used to derive the
    /// `open_<name>_index` helper method emitted by the derive.
    pub name: &'static str,
    /// Cardinality of the relationship between the entity and the
    /// secondary key.
    pub relate: Relate,
    /// Optional related-entity class name (for foreign-key relationships).
    /// `None` means "no foreign-key constraint".
    pub related_entity: Option<&'static str>,
    /// Action to take when an entity referenced by this foreign-key
    /// secondary index is deleted from its primary store.  Defaults to
    /// `Abort` if not specified.
    pub on_related_entity_delete: DeleteAction,
}

impl SecondarySpec {
    /// Constant-fn constructor used by the derive to populate the
    /// `SECONDARY_INDEXES` const table.  Public so user code can also
    /// build a metadata table by hand if it ever needs to bypass the
    /// derive.
    pub const fn new(name: &'static str, relate: Relate) -> Self {
        Self {
            name,
            relate,
            related_entity: None,
            on_related_entity_delete: DeleteAction::Abort,
        }
    }

    /// Builder: attach a `related_entity` foreign-key reference.
    pub const fn with_related_entity(
        mut self,
        related_entity: &'static str,
    ) -> Self {
        self.related_entity = Some(related_entity);
        self
    }

    /// Builder: choose the action to take when the related entity is
    /// deleted.
    pub const fn with_on_related_entity_delete(
        mut self,
        action: DeleteAction,
    ) -> Self {
        self.on_related_entity_delete = action;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_new_defaults() {
        let s = SecondarySpec::new("by_email", Relate::OneToOne);
        assert_eq!(s.name, "by_email");
        assert_eq!(s.relate, Relate::OneToOne);
        assert_eq!(s.related_entity, None);
        assert_eq!(s.on_related_entity_delete, DeleteAction::Abort);
    }

    #[test]
    fn spec_builders() {
        let s = SecondarySpec::new("by_dept", Relate::ManyToOne)
            .with_related_entity("Department")
            .with_on_related_entity_delete(DeleteAction::Nullify);
        assert_eq!(s.related_entity, Some("Department"));
        assert_eq!(s.on_related_entity_delete, DeleteAction::Nullify);
    }

    #[test]
    fn relate_all_variants() {
        // Smoke-test pattern matching across all variants.
        for r in [
            Relate::OneToOne,
            Relate::ManyToOne,
            Relate::OneToMany,
            Relate::ManyToMany,
        ] {
            let s = SecondarySpec::new("x", r);
            match s.relate {
                Relate::OneToOne
                | Relate::ManyToOne
                | Relate::OneToMany
                | Relate::ManyToMany => {}
            }
        }
    }

    #[test]
    fn delete_action_all_variants() {
        for a in
            [DeleteAction::Abort, DeleteAction::Cascade, DeleteAction::Nullify]
        {
            let s = SecondarySpec::new("x", Relate::OneToOne)
                .with_on_related_entity_delete(a);
            match s.on_related_entity_delete {
                DeleteAction::Abort
                | DeleteAction::Cascade
                | DeleteAction::Nullify => {}
            }
        }
    }
}
