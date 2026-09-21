//! Python/JSON wire conversion: the direct serde serializer and dict helpers.

use crate::prelude::*;

pub(crate) fn py_dict_item<'py>(
    dict: &Bound<'py, PyDict>,
    key: &str,
) -> Result<Option<Bound<'py, PyAny>>, String> {
    dict.get_item(key)
        .map_err(|_| format!("{key} lookup failed"))
}

pub(crate) fn py_dict_key_as_string(
    key: &Bound<'_, PyAny>,
) -> Result<String, String> {
    key.extract::<String>()
        .map_err(|_| "dict keys must be strings".to_string())
}

pub(crate) fn json_value_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<serde_json::Value> {
    py_to_json_value(dict.as_any())
}

pub(crate) fn strings_to_paths(paths: Vec<String>) -> Vec<PathBuf> {
    paths.into_iter().map(PathBuf::from).collect()
}

pub(crate) fn json_record_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<BTreeMap<String, JsonValue>> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "record is not a JSON object with string keys: {e}"
        ))
    })
}

/// Convert a Python value (dict / list / str / number / bool / None) into
/// a `serde_json::Value`. Used to deserialize ChangeSpecWire dicts coming
/// in from the Python side of `evaluate_query_many`.
pub(crate) fn py_to_json_value(
    value: &Bound<'_, PyAny>,
) -> PyResult<JsonValue> {
    if value.is_none() {
        return Ok(JsonValue::Null);
    }
    if let Ok(b) = value.extract::<bool>() {
        return Ok(JsonValue::Bool(b));
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(JsonValue::Number(i.into()));
    }
    if let Ok(u) = value.extract::<u64>() {
        return Ok(JsonValue::Number(u.into()));
    }
    if let Ok(f) = value.extract::<f64>() {
        return serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .ok_or_else(|| {
                PyValueError::new_err(format!("non-finite float: {f}"))
            });
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(JsonValue::String(s));
    }
    if let Ok(list) = value.downcast::<PyList>() {
        let mut arr = Vec::with_capacity(list.len());
        for item in list.iter() {
            arr.push(py_to_json_value(&item)?);
        }
        return Ok(JsonValue::Array(arr));
    }
    if let Ok(tuple) = value.downcast::<PyTuple>() {
        let mut arr = Vec::with_capacity(tuple.len());
        for item in tuple.iter() {
            arr.push(py_to_json_value(&item)?);
        }
        return Ok(JsonValue::Array(arr));
    }
    if let Ok(dict) = value.downcast::<PyDict>() {
        let mut obj = JsonMap::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let key: String = k.extract().map_err(|_| {
                PyValueError::new_err("dict keys must be strings")
            })?;
            obj.insert(key, py_to_json_value(&v)?);
        }
        return Ok(JsonValue::Object(obj));
    }
    Err(PyValueError::new_err(format!(
        "unsupported value of type {}",
        value.get_type().name()?
    )))
}

pub(crate) fn strings_from_py_list(
    list: &Bound<'_, PyList>,
    label: &str,
) -> PyResult<Vec<String>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        values.push(item.extract::<String>().map_err(|_| {
            PyValueError::new_err(format!("{label}[{idx}] must be a string"))
        })?);
    }
    Ok(values)
}

pub(crate) fn hooks_from_py(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<HookWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        values.push(serde_json::from_value(py_to_json_value(&item)?).map_err(
            |e| {
                PyValueError::new_err(format!(
                    "hooks[{idx}] is not a valid HookWire dict: {e}"
                ))
            },
        )?);
    }
    Ok(values)
}

pub(crate) fn mentors_from_py(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<MentorWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        values.push(serde_json::from_value(py_to_json_value(&item)?).map_err(
            |e| {
                PyValueError::new_err(format!(
                    "mentors[{idx}] is not a valid MentorWire dict: {e}"
                ))
            },
        )?);
    }
    Ok(values)
}

pub(crate) fn comments_from_py(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<CommentWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        values.push(serde_json::from_value(py_to_json_value(&item)?).map_err(
            |e| {
                PyValueError::new_err(format!(
                    "comments[{idx}] is not a valid CommentWire dict: {e}"
                ))
            },
        )?);
    }
    Ok(values)
}

pub(crate) fn json_value_to_py<'py>(
    py: Python<'py>,
    value: &JsonValue,
) -> PyResult<PyObject> {
    match value {
        JsonValue::Null => Ok(py.None()),
        JsonValue::Bool(b) => Ok(b.into_py(py)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_py(py))
            } else if let Some(u) = n.as_u64() {
                Ok(u.into_py(py))
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_py(py))
            } else {
                // Should be unreachable for serde_json numbers.
                Err(PyValueError::new_err(format!(
                    "unrepresentable JSON number: {n}"
                )))
            }
        }
        JsonValue::String(s) => Ok(s.into_py(py)),
        JsonValue::Array(arr) => json_array_to_py(py, arr),
        JsonValue::Object(obj) => json_object_to_py(py, obj),
    }
}

fn json_array_to_py<'py>(
    py: Python<'py>,
    arr: &[JsonValue],
) -> PyResult<PyObject> {
    let list = PyList::empty_bound(py);
    for v in arr {
        list.append(json_value_to_py(py, v)?)?;
    }
    Ok(list.into())
}

fn json_object_to_py<'py>(
    py: Python<'py>,
    obj: &JsonMap<String, JsonValue>,
) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    for (k, v) in obj {
        dict.set_item(k, json_value_to_py(py, v)?)?;
    }
    Ok(dict.into())
}

#[derive(Debug)]
struct PySerializeError(String);

impl std::fmt::Display for PySerializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PySerializeError {}

impl ser::Error for PySerializeError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

impl From<PyErr> for PySerializeError {
    fn from(error: PyErr) -> Self {
        Self(error.to_string())
    }
}

pub(crate) fn serialize_to_py<'py, T>(
    py: Python<'py>,
    value: &T,
) -> PyResult<PyObject>
where
    T: serde::Serialize + ?Sized,
{
    value.serialize(PySerializer { py }).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })
}

#[derive(Clone, Copy)]
struct PySerializer<'py> {
    py: Python<'py>,
}

impl<'py> Serializer for PySerializer<'py> {
    type Ok = PyObject;
    type Error = PySerializeError;
    type SerializeSeq = PyListSerializer<'py>;
    type SerializeTuple = PyListSerializer<'py>;
    type SerializeTupleStruct = PyListSerializer<'py>;
    type SerializeTupleVariant = PyTupleVariantSerializer<'py>;
    type SerializeMap = PyDictSerializer<'py>;
    type SerializeStruct = PyDictSerializer<'py>;
    type SerializeStructVariant = PyStructVariantSerializer<'py>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(v.into_py(self.py))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(v.into_py(self.py))
    }

    fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
        let narrowed = i64::try_from(v).map_err(|_| {
            <PySerializeError as ser::Error>::custom(format!(
                "integer out of JSON range: {v}"
            ))
        })?;
        self.serialize_i64(narrowed)
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(v.into_py(self.py))
    }

    fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
        let narrowed = u64::try_from(v).map_err(|_| {
            <PySerializeError as ser::Error>::custom(format!(
                "integer out of JSON range: {v}"
            ))
        })?;
        self.serialize_u64(narrowed)
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        if v.is_finite() {
            Ok(v.into_py(self.py))
        } else {
            Ok(self.py.None())
        }
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(&v.to_string())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(v.into_py(self.py))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        let list = PyList::empty_bound(self.py);
        for byte in v {
            list.append(*byte).map_err(PySerializeError::from)?;
        }
        Ok(list.into())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.py.None())
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.py.None())
    }

    fn serialize_unit_struct(
        self,
        _name: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(self.py.None())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        let dict = PyDict::new_bound(self.py);
        dict.set_item(variant, value.serialize(self)?)
            .map_err(PySerializeError::from)?;
        Ok(dict.into())
    }

    fn serialize_seq(
        self,
        _len: Option<usize>,
    ) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(PyListSerializer {
            py: self.py,
            list: PyList::empty_bound(self.py),
        })
    }

    fn serialize_tuple(
        self,
        len: usize,
    ) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(PyTupleVariantSerializer {
            py: self.py,
            variant,
            list: PyList::empty_bound(self.py),
        })
    }

    fn serialize_map(
        self,
        _len: Option<usize>,
    ) -> Result<Self::SerializeMap, Self::Error> {
        Ok(PyDictSerializer {
            py: self.py,
            dict: PyDict::new_bound(self.py),
            next_key: None,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(PyDictSerializer {
            py: self.py,
            dict: PyDict::new_bound(self.py),
            next_key: None,
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(PyStructVariantSerializer {
            py: self.py,
            variant,
            dict: PyDict::new_bound(self.py),
        })
    }
}

struct PyListSerializer<'py> {
    py: Python<'py>,
    list: Bound<'py, PyList>,
}

impl<'py> SerializeSeq for PyListSerializer<'py> {
    type Ok = PyObject;
    type Error = PySerializeError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        self.list
            .append(value.serialize(PySerializer { py: self.py })?)
            .map_err(PySerializeError::from)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.list.into())
    }
}

impl<'py> SerializeTuple for PyListSerializer<'py> {
    type Ok = PyObject;
    type Error = PySerializeError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        SerializeSeq::end(self)
    }
}

impl<'py> SerializeTupleStruct for PyListSerializer<'py> {
    type Ok = PyObject;
    type Error = PySerializeError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        SerializeSeq::end(self)
    }
}

struct PyTupleVariantSerializer<'py> {
    py: Python<'py>,
    variant: &'static str,
    list: Bound<'py, PyList>,
}

impl<'py> SerializeTupleVariant for PyTupleVariantSerializer<'py> {
    type Ok = PyObject;
    type Error = PySerializeError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        self.list
            .append(value.serialize(PySerializer { py: self.py })?)
            .map_err(PySerializeError::from)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let dict = PyDict::new_bound(self.py);
        dict.set_item(self.variant, self.list)
            .map_err(PySerializeError::from)?;
        Ok(dict.into())
    }
}

struct PyDictSerializer<'py> {
    py: Python<'py>,
    dict: Bound<'py, PyDict>,
    next_key: Option<String>,
}

impl<'py> SerializeMap for PyDictSerializer<'py> {
    type Ok = PyObject;
    type Error = PySerializeError;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        if self.next_key.is_some() {
            return Err(<PySerializeError as ser::Error>::custom(
                "map key serialized before previous value",
            ));
        }
        self.next_key = Some(key.serialize(PyMapKeySerializer)?);
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        let key = self.next_key.take().ok_or_else(|| {
            <PySerializeError as ser::Error>::custom(
                "map value serialized before key",
            )
        })?;
        self.dict
            .set_item(key, value.serialize(PySerializer { py: self.py })?)
            .map_err(PySerializeError::from)
    }

    fn serialize_entry<K, V>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), Self::Error>
    where
        K: ?Sized + serde::Serialize,
        V: ?Sized + serde::Serialize,
    {
        self.dict
            .set_item(
                key.serialize(PyMapKeySerializer)?,
                value.serialize(PySerializer { py: self.py })?,
            )
            .map_err(PySerializeError::from)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        if self.next_key.is_some() {
            return Err(<PySerializeError as ser::Error>::custom(
                "map key serialized without value",
            ));
        }
        Ok(self.dict.into())
    }
}

impl<'py> SerializeStruct for PyDictSerializer<'py> {
    type Ok = PyObject;
    type Error = PySerializeError;

    fn serialize_field<T>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        self.dict
            .set_item(key, value.serialize(PySerializer { py: self.py })?)
            .map_err(PySerializeError::from)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.dict.into())
    }
}

struct PyStructVariantSerializer<'py> {
    py: Python<'py>,
    variant: &'static str,
    dict: Bound<'py, PyDict>,
}

impl<'py> SerializeStructVariant for PyStructVariantSerializer<'py> {
    type Ok = PyObject;
    type Error = PySerializeError;

    fn serialize_field<T>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        self.dict
            .set_item(key, value.serialize(PySerializer { py: self.py })?)
            .map_err(PySerializeError::from)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let outer = PyDict::new_bound(self.py);
        outer
            .set_item(self.variant, self.dict)
            .map_err(PySerializeError::from)?;
        Ok(outer.into())
    }
}

struct PyMapKeySerializer;

impl Serializer for PyMapKeySerializer {
    type Ok = String;
    type Error = PySerializeError;
    type SerializeSeq = Impossible<String, PySerializeError>;
    type SerializeTuple = Impossible<String, PySerializeError>;
    type SerializeTupleStruct = Impossible<String, PySerializeError>;
    type SerializeTupleVariant = Impossible<String, PySerializeError>;
    type SerializeMap = Impossible<String, PySerializeError>;
    type SerializeStruct = Impossible<String, PySerializeError>;
    type SerializeStructVariant = Impossible<String, PySerializeError>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(if v { "true" } else { "false" }.to_string())
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        if v.is_finite() {
            Ok(serde_json::Number::from_f64(v)
                .ok_or_else(|| {
                    <PySerializeError as ser::Error>::custom(
                        "non-finite map key",
                    )
                })?
                .to_string())
        } else {
            Err(<PySerializeError as ser::Error>::custom(
                "non-finite map key",
            ))
        }
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string())
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(<PySerializeError as ser::Error>::custom(
            "map keys must serialize as strings",
        ))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(<PySerializeError as ser::Error>::custom(
            "map keys must serialize as strings",
        ))
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(<PySerializeError as ser::Error>::custom(
            "map keys must serialize as strings",
        ))
    }

    fn serialize_unit_struct(
        self,
        _name: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(<PySerializeError as ser::Error>::custom(
            "map keys must serialize as strings",
        ))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(variant.to_string())
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        Err(<PySerializeError as ser::Error>::custom(
            "map keys must serialize as strings",
        ))
    }

    fn serialize_seq(
        self,
        _len: Option<usize>,
    ) -> Result<Self::SerializeSeq, Self::Error> {
        Err(<PySerializeError as ser::Error>::custom(
            "map keys must serialize as strings",
        ))
    }

    fn serialize_tuple(
        self,
        _len: usize,
    ) -> Result<Self::SerializeTuple, Self::Error> {
        Err(<PySerializeError as ser::Error>::custom(
            "map keys must serialize as strings",
        ))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(<PySerializeError as ser::Error>::custom(
            "map keys must serialize as strings",
        ))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(<PySerializeError as ser::Error>::custom(
            "map keys must serialize as strings",
        ))
    }

    fn serialize_map(
        self,
        _len: Option<usize>,
    ) -> Result<Self::SerializeMap, Self::Error> {
        Err(<PySerializeError as ser::Error>::custom(
            "map keys must serialize as strings",
        ))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(<PySerializeError as ser::Error>::custom(
            "map keys must serialize as strings",
        ))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(<PySerializeError as ser::Error>::custom(
            "map keys must serialize as strings",
        ))
    }
}

#[cfg(test)]
mod tests;
