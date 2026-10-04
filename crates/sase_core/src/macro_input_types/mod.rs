//! Macro input-type catalog, resolver, choice rules, and value checks.

mod catalog;
mod choices;
mod pyyaml;
mod registry;
mod resolve;
mod suggest;
mod value;

pub use catalog::{
    builtin_catalog, CatalogEntry, CatalogSource, InputTypeKind,
};
pub use choices::{
    validate_enum_choices, validate_enum_choices_yaml, ChoiceIssue,
    ChoiceIssueSeverity, InputChoice, ValidateEnumChoicesResult,
};
pub use pyyaml::{
    pyyaml_plain_scalar_is_non_string, unquoted_plain_scalar_choice_error,
    PyyamlScalarKind,
};
pub use registry::InputTypeRegistry;
pub use resolve::{
    resolve_input_type, ResolveInputTypeError, ResolvedInputType,
};
pub use suggest::{did_you_mean_suffix, suggest_closest};
pub use value::{check_closed_set_default, check_input_value};

#[cfg(test)]
mod tests;
