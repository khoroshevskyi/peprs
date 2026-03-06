use polars::prelude::*;
use pyo3::prelude::*;

pub fn anyvalue_to_pyobject(py: Python<'_>, value: &AnyValue) -> PyObject {
    match value {
        AnyValue::Null => py.None(),
        AnyValue::Boolean(b) => b.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        AnyValue::Int8(i) => i.into_pyobject(py).unwrap().into_any().unbind(),
        AnyValue::Int16(i) => i.into_pyobject(py).unwrap().into_any().unbind(),
        AnyValue::Int32(i) => i.into_pyobject(py).unwrap().into_any().unbind(),
        AnyValue::Int64(i) => i.into_pyobject(py).unwrap().into_any().unbind(),
        AnyValue::UInt8(i) => i.into_pyobject(py).unwrap().into_any().unbind(),
        AnyValue::UInt16(i) => i.into_pyobject(py).unwrap().into_any().unbind(),
        AnyValue::UInt32(i) => i.into_pyobject(py).unwrap().into_any().unbind(),
        AnyValue::UInt64(i) => i.into_pyobject(py).unwrap().into_any().unbind(),
        AnyValue::Float32(f) => f.into_pyobject(py).unwrap().into_any().unbind(),
        AnyValue::Float64(f) => f.into_pyobject(py).unwrap().into_any().unbind(),
        AnyValue::String(s) => s.into_pyobject(py).unwrap().into_any().unbind(),
        AnyValue::StringOwned(s) => s.as_str().into_pyobject(py).unwrap().into_any().unbind(),
        _ => value
            .to_string()
            .into_pyobject(py)
            .unwrap()
            .into_any()
            .unbind(),
    }
}
