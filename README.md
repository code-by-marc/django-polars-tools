# django_polars_tools

Utilities for integrating **Django** and **Polars**, including safe
QuerySet → DataFrame conversion, correct schema inference, and
nullable field handling.

This package solves the common issue where Polars incorrectly infers
nullable fields when converting Django QuerySets, especially when using
`infer_schema_length`. `django_polars_tools` provides reliable schema
handling and high-performance data extraction from Django models.

---

## 🚀 Features

- **Safe QuerySet → Polars DataFrame conversion**
- **Correct handling of nullable fields**
- **Improved schema inference compared to Polars defaults**
- **Fast extraction path for large querysets**
- Simple API designed to “just work”
- Django-friendly, Polars-native

More features will be added as the project grows toward deeper Django ↔ Polars interoperability.

---

## 📦 Installation

```bash
pip install django_polars_tools
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv add django_polars_tools
```

## 📝 Why this library?

Polars' schema inference works great for many cases, but with Django
querysets it can:

- infer nullable fields incorrectly
- misclassify types with limited sample size
- incorrectly infer schema when using infer_schema_length

This library provides consistent handling tailored for the Django ORM.

## 🤝 Contributing
Contributions are welcome!

Open an issue or submit a PR if you’d like to help improve the project.
