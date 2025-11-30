from datetime import datetime, timedelta, timezone
from typing import Any

import django
import polars as pl
import pytest
from django.apps import apps
from django.conf import settings
from django.db import connection, models

from django_polars_tools import main as polars_schema_django


@pytest.fixture(scope="session", autouse=True)
def django_setup() -> None:
    """Dynamically configure Django settings for the test session."""
    if not settings.configured:
        settings.configure(
            DATABASES={
                "default": {
                    "ENGINE": "django.db.backends.sqlite3",
                    "NAME": ":memory:",
                }
            },
            INSTALLED_APPS=["__main__"],
            USE_TZ=True,
        )
    django.setup()


@pytest.fixture()
def test_model() -> Any:
    """Create a temporary Django model for testing."""

    class TestModel(models.Model):
        class Meta:
            app_label = "__main__"

        # auto_field = models.AutoField()  # Auto field not required to test
        # big_auto_field = models.BigAutoField()
        big_integer_field = models.BigIntegerField(null=True)
        binary_field = models.BinaryField(null=True)
        boolean_field = models.BooleanField(null=True)
        char_field = models.CharField(
            null=True,
            max_length=255,
        )
        date_field = models.DateField(null=True)
        date_time_field = models.DateTimeField(null=True)
        decimal_field = models.DecimalField(null=True, max_digits=5, decimal_places=2)
        duration_field = models.DurationField(null=True)
        email_field = models.EmailField(null=True)
        # file_field = models.FileField(null=True)
        file_path_field = models.FilePathField(null=True)
        float_field = models.FloatField(null=True)
        # generated_field = models.GeneratedField()
        generic_ip_address_field = models.GenericIPAddressField(null=True)
        # image_field = (
        #     models.ImageField()
        # )  # (fields.E210) Cannot use ImageField because Pillow is not installed
        integer_field = models.IntegerField(null=True)
        # json_field = models.JSONField(null=True)
        positive_big_integer_field = models.PositiveBigIntegerField(null=True)
        positive_integer_field = models.PositiveIntegerField(null=True)
        positive_small_integer_field = models.PositiveSmallIntegerField(null=True)
        slug_field = models.SlugField(null=True)
        # small_auto_field = models.SmallAutoField()  # Auto field not required to test
        small_integer_field = models.SmallIntegerField(null=True)
        text_field = models.TextField(null=True)
        time_field = models.TimeField(null=True)
        url_field = models.URLField(null=True)
        # uuid_field = models.UUIDField(null=True)
        # foreign_key_field = models.ForeignKey()
        # many_to_many_field = models.ManyToManyField()
        # one_to_one_field = models.OneToOneField()

    with connection.schema_editor() as schema_editor:
        schema_editor.create_model(TestModel)

    yield TestModel

    # Clean up: delete table and unregister model from app registry
    with connection.schema_editor() as schema_editor:
        schema_editor.delete_model(TestModel)

    # Unregister the model to avoid reloading warning
    app_config = apps.get_app_config("__main__")
    if "testmodel" in app_config.models:
        del app_config.models["testmodel"]




@pytest.fixture()
def book_models() -> Any:
    """Create a temporary Django model for testing."""

    class AuthorModel(models.Model):
        class Meta:
            app_label = "__main__"

        name = models.CharField(max_length=100, null=True)

    class BookModel(models.Model):
        class Meta:
            app_label = "__main__"

        title = models.CharField(max_length=100, null=True)
        author = models.ForeignKey(AuthorModel, on_delete=models.CASCADE, null=True)

    with connection.schema_editor() as schema_editor:
        schema_editor.create_model(AuthorModel)
        schema_editor.create_model(BookModel)

    yield AuthorModel, BookModel

    # Clean up: delete table and unregister model from app registry
    with connection.schema_editor() as schema_editor:
        schema_editor.delete_model(BookModel)  # Delete child table first
        schema_editor.delete_model(AuthorModel)

    # Unregister the model to avoid reloading warning
    app_config = apps.get_app_config("__main__")
    if "bookmodel" in app_config.models:
        del app_config.models["bookmodel"]
    if "authormodel" in app_config.models:
        del app_config.models["authormodel"]


@pytest.fixture
def test_data(test_model: models.Model) -> list[dict[str, Any]]:
    test_model.objects.create()
    test_model.objects.create(
        # auto_field = None,  # Not required to test
        # big_auto_field = None,  # Not required to test
        big_integer_field=9_223_372_036_854_775_807,
        binary_field=b"110100100",
        boolean_field=True,
        char_field="Hello World!",
        date_field=datetime.now(tz=timezone.utc),
        date_time_field=datetime.now(tz=timezone.utc),
        decimal_field=99.99,
        duration_field=timedelta(seconds=30),
        email_field="example@domain.com",
        # file_field=None,  # TODO: Figure out how to test file_field
        file_path_field="/path/to/your/hearth",
        float_field=0.99,
        # generated_field = models.GeneratedField(),  # TODO: Figure out if this needs to be tested.
        generic_ip_address_field="127.0.0.1",
        # image_field=None,  # TODO: (fields.E210) Cannot use ImageField because Pillow is not installed
        integer_field=2_147_483_647,
        # json_field=json.dumps({"key": "value"}),
        positive_big_integer_field=9_223_372_036_854_775_807,
        positive_integer_field=2_147_483_647,
        positive_small_integer_field=32_767,
        slug_field="hello,world",
        # small_auto_field = models.SmallAutoField()  # Not required to test
        small_integer_field=32_767,
        text_field="This is some text.",
        time_field=datetime.now(tz=timezone.utc),
        url_field="www.example.com",
        # uuid_field=uuid4(),
    )
    return list(test_model.objects.all().values())


@pytest.fixture
def book_data(book_models: models.Model) -> list[dict[str, Any]]:
    author_model, book_model = book_models
    author1 = author_model.objects.create()
    author1.save()
    book1 = book_model.objects.create()
    book1.save()

    author2 = author_model.objects.create(name="John Doe")
    author2.save()
    book2 = book_model.objects.create(title="Sample Book", author=author2)
    book2.save()

    return list(book_model.objects.all().values())


def test_queryset(test_model: models.Model, test_data: list[dict[str, Any]]) -> None:
    queryset = test_model.objects.all()
    df = polars_schema_django.django_queryset_to_dataframe(queryset)
    assert isinstance(df, pl.DataFrame)


def test_queryset_annotation(
    test_model: models.Model, test_data: list[dict[str, Any]]
) -> None:
    queryset = test_model.objects.all().annotate(
        char_field_upper=models.functions.Upper("char_field"),
        char_field_length=models.functions.Length("char_field"),
    )
    df = polars_schema_django.django_queryset_to_dataframe(queryset)
    assert isinstance(df, pl.DataFrame)


def test_queryset_values_and_annotations(
    test_model: models.Model, test_data: list[dict[str, Any]]
) -> None:
    queryset = (
        test_model.objects.all()
        .values(
            "big_integer_field",
            "binary_field",
            "boolean_field",
            "char_field",
            "date_field",
            "date_time_field",
            "decimal_field",
            "duration_field",
            "email_field",
        )
        .annotate(
            char_field_upper=models.functions.Upper("char_field"),
            char_field_length=models.functions.Length("char_field"),
        )
    )
    df = polars_schema_django.django_queryset_to_dataframe(
        queryset, infer_schema_length=1
    )
    assert isinstance(df, pl.DataFrame)


def test_queryset_relation(
    book_models: models.Model, book_data: list[dict[str, Any]]
) -> None:
    author_model, book_model = book_models
    queryset = book_model.objects.all().values(
        "title",
        "author__name",
    )
    df = polars_schema_django.django_queryset_to_dataframe(
        queryset, infer_schema_length=1
    )
    assert isinstance(df, pl.DataFrame)
