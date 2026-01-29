#!/usr/bin/env python3
import polars as pl
from django.db import connections, models

# Type alias for Polars data types (instances or type classes)
PolarsType = pl.DataType | type[pl.DataType]

# Default mapping from Django field types to Polars data types
DJANGO_MAPPING: dict[type[models.Field], PolarsType | None] = {
    models.AutoField: pl.Int32,
    models.BigAutoField: pl.Int64,
    models.BigIntegerField: pl.Int64,
    models.BinaryField: pl.Binary,
    models.BooleanField: pl.Boolean,
    models.CharField: pl.String,
    models.DateField: pl.Date,
    models.DateTimeField: pl.Datetime,
    models.DecimalField: pl.Decimal,  # Scale and precision handled at runtime
    models.DurationField: pl.Duration,
    models.EmailField: pl.String,
    models.FileField: None,
    models.FilePathField: pl.String,
    models.FloatField: pl.Float64,
    models.GenericIPAddressField: pl.String,
    models.ImageField: None,
    models.IntegerField: pl.Int32,
    models.JSONField: pl.String,
    models.PositiveBigIntegerField: pl.Int64,
    models.PositiveIntegerField: pl.Int32,
    models.PositiveSmallIntegerField: pl.Int16,
    models.SlugField: pl.String,
    models.SmallAutoField: pl.Int16,
    models.SmallIntegerField: pl.Int16,
    models.TextField: pl.String,
    models.TimeField: pl.Time,
    models.URLField: pl.String,
    models.UUIDField: pl.Object,
    models.ForeignKey: None,  # ForeignKey fields cannot be used directly
    models.ManyToManyField: None,  # ManyToMany fields cannot be used directly
    models.OneToOneField: None,  # OneToOne fields cannot be used directly
    models.fields.proxy.OrderWrt: pl.Int32,
}

# Add GeneratedField support for Django 5.0+
if hasattr(models, "GeneratedField"):
    DJANGO_MAPPING[models.GeneratedField] = None


def _concrete_fields_to_django_schema(fields) -> dict[str, models.Field]:
    """Convert a list of Django fields to a Django schema."""
    output = {}
    for field in fields:
        field_class = field
        if field.is_relation:
            field_class = field.target_field

        output[field.column] = field_class

    return output


def _queryset_to_polars_schema(
    django_schema: dict[str, models.Field],
    django_to_polars_mapping: dict[type[models.Field], PolarsType | None],
) -> dict[str, PolarsType]:
    polars_schema: dict[str, PolarsType] = {}
    for column_name, django_field_class in django_schema.items():
        polars_dtype = django_to_polars_mapping.get(django_field_class.__class__)
        if polars_dtype == pl.Decimal:
            # Handle DecimalField separately as it requires scale and precision
            polars_schema[column_name] = pl.Decimal(
                scale=getattr(django_field_class, "decimal_places", 0),
                precision=getattr(django_field_class, "max_digits", None),
            )
        elif polars_dtype is not None:
            polars_schema[column_name] = polars_dtype
        else:
            raise ValueError(
                f"No mapping for Django field type {django_field_class} "
                f"for column '{column_name}'"
            )
    return polars_schema


def _queryset_to_django_schema(queryset: models.QuerySet) -> dict[str, models.Field]:
    # When using .all(), .filter(), etc., default_cols is True
    # .get_select() will not contain the names to the fields
    # Need addition to get annotations as well
    if queryset.query.default_cols:
        schema = _concrete_fields_to_django_schema(
            queryset.query.get_meta().concrete_fields
        )

        for alias, annotation in queryset.query.annotations.items():
            schema[alias] = annotation.output_field
        return schema

    # Using .values() then .get_select() will contain the selected fields only
    schema = {}
    selected_fields = queryset.query.get_compiler(using=queryset.db).get_select()[0]
    for field in selected_fields:
        # field[2] is the alias, field[0].field is the Django field
        # In Django 4.2, alias can be None for some fields, use the field's column name
        alias = field[2] or field[0].field.column
        schema[alias] = field[0].field
    return schema


def _read_database(
    queryset: models.QuerySet, schema: dict[str, PolarsType], **kwargs
) -> pl.DataFrame:
    sql, params = queryset.query.sql_with_params()

    with connections[queryset.db].cursor() as cursor:
        return pl.read_database(  # type: ignore[no-any-return]
            query=sql,
            connection=cursor,
            execute_options={"params": params or ()},
            schema_overrides=schema,
            **kwargs,
        )


def django_queryset_to_dataframe(
    queryset: models.QuerySet,
    mapping: dict[type[models.Field], PolarsType | None] | None = None,
    **kwargs,
) -> pl.DataFrame:
    mapping = mapping or DJANGO_MAPPING
    django_schema = _queryset_to_django_schema(queryset)
    polars_schema = _queryset_to_polars_schema(django_schema, mapping)
    return _read_database(queryset, polars_schema, **kwargs)
