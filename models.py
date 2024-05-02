from pydantic import BaseModel


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


class ColumnToChange(BaseModel):
    name: str
    subtype: str | None = None
    regex: str | None = None
