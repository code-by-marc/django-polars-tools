# django-polars-tools

[![Tests](https://github.com/code-by-marc/django-polars-tools/actions/workflows/test.yml/badge.svg)](https://github.com/code-by-marc/django-polars-tools/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/code-by-marc/django-polars-tools/graph/badge.svg?token=H7VZBZSDPY)](https://codecov.io/gh/code-by-marc/django-polars-tools)
[![PyPI version](https://badge.fury.io/py/django-polars-tools.svg)](https://badge.fury.io/py/django-polars-tools)
[![Python versions](https://img.shields.io/pypi/pyversions/django-polars-tools.svg)](https://pypi.org/project/django-polars-tools/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Utilities for integrating **Django** and **Polars**, including safe
QuerySet → DataFrame conversion, correct schema inference, and
nullable field handling.

This package solves the common issue where Polars incorrectly infers
nullable fields when converting Django QuerySets, especially when using
`infer_schema_length`. `django_polars_tools` provides reliable schema
handling and high-performance data extraction from Django models.

## 🚀 Features

- **Safe QuerySet → Polars DataFrame conversion**
- **Correct handling of nullable fields**
- **Improved schema inference compared to Polars defaults**
- **Fast extraction path for large querysets**
- Simple API designed to “just work”
- Django-friendly, Polars-native

More features will be added as the project grows toward deeper Django ↔ Polars interoperability.

## 📦 Installation

```bash
pip install django-polars-tools
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv add django-polars-tools
```

## 📚 Usage

### Basic Example

Convert any Django QuerySet to a Polars DataFrame:

```python
from django_polars_tools import django_queryset_to_dataframe
from myapp.models import MyModel

# Get a queryset
queryset = MyModel.objects.filter(active=True)

# Convert to Polars DataFrame
df = django_queryset_to_dataframe(queryset)
```

### With Annotations

Works seamlessly with Django annotations:

```python
from django.db.models import Count, F
from django_polars_tools import django_queryset_to_dataframe

queryset = MyModel.objects.annotate(
    total=Count('items'),
    display_name=F('first_name') + ' ' + F('last_name')
)

df = django_queryset_to_dataframe(queryset)
```

### Using .values()

Also supports QuerySets with `.values()`:

```python
queryset = MyModel.objects.values('id', 'name', 'created_at')
df = django_queryset_to_dataframe(queryset)
```

### Custom Field Mapping

Override the default Django → Polars type mapping:

```python
import polars as pl
from django.db import models
from django_polars_tools import django_queryset_to_dataframe, DJANGO_MAPPING

# Create custom mapping
custom_mapping = DJANGO_MAPPING.copy()
custom_mapping[models.JSONField] = pl.Object  # Store JSON as Object instead of String

df = django_queryset_to_dataframe(queryset, mapping=custom_mapping)
```

### Additional Polars Options

Pass any `pl.read_database()` kwargs:

```python
df = django_queryset_to_dataframe(
    queryset,
    batch_size=10000,
    # Other polars.read_database options...
)
```

## 📝 Why this library?

Polars' schema inference works great for many cases, but with Django
querysets it can:

- infer nullable fields incorrectly
- misclassify types with limited sample size

This library provides consistent handling tailored for the Django ORM.

## 🤝 Contributing
Contributions are welcome!

Open an issue or submit a PR if you’d like to help improve the project.
