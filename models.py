from pydantic import BaseModel


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
    subtype: str | None
    regex: str | None


class TableChangeSettings(BaseModel):
    table_name: str
    columns_to_change: list[ColumnChangeSettings]
