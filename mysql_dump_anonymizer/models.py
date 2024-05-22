from typing import Any

from pydantic import BaseModel, model_validator
from pydantic_core import PydanticCustomError


class ForeignKeyReference(BaseModel):
    table_name: str
    column_name: str


class ForeignKeyData(BaseModel):
    column_name: str
    referenced_table_name: str
    referenced_column_name: str


class TableColumn(BaseModel):
    name: str
    sql_data_type: str


class TableData(BaseModel):
    table_name: str
    table_columns: list[TableColumn]
    foreign_keys: list[ForeignKeyData]


class ColumnChangeSettings(BaseModel):
    name: str
    subtype: str | None = None
    regex: str | None = None
    interval: tuple[float, float] | None = None

    @model_validator(mode="after")
    def maximum_one_of_subtype_and_regex_should_be_set(self: "ColumnChangeSettings") -> "ColumnChangeSettings":
        if self.subtype is not None and self.regex is not None:
            raise PydanticCustomError(
                "InputException",
                "subtype and regex cannot both be set, got subtype={subtype}, regex={regex}",
                dict(subtype=self.subtype, regex=self.regex),
            )
        return self
    #
    # @model_validator(mode="before")
    # @classmethod
    # def set_attribute_as_none_if_key_missing_in_json(cls, values: dict[str, Any]):
    #     if values.get("regex") is None:
    #         values["regex"] = None
    #     if values.get("subtype") is None:
    #         values["subtype"] = None
    #     if values.get("interval") is None:
    #         values["interval"] = None
    #     return values


class TableChangeSettings(BaseModel):
    table_name: str
    columns_to_change: list[ColumnChangeSettings]
