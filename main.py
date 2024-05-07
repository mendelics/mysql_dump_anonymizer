import re
import time
from typing import Any

from faker import Faker
from models import ForeignKeyData, TableColumn, TableData, ForeignKeyReference

time_char_by_char = 0
time_regex = 0

time_total = 0
time_update_changes_dict = 0


def line_to_list_regex(line: str) -> list[str]:
    global time_regex
    begin = time.time()
    clean_line = line.strip("() ;").replace("`", "")
    ret = re.findall("'[^']*'|[^,]+(?=,)|(?<=\")[^,]+(?=,)|(?<=,)[^,]+", clean_line)
    time_regex += time.time() - begin
    return ret


def parse_table_structure(table_name: str, file) -> TableData:
    table_columns = []
    foreign_keys = []
    for line in file:
        line = line.decode("utf-8").lower().strip()
        accepted_line_starts = ["`", "primary key", "unique key", "key", "constraint"]
        if not any(line.startswith(start) for start in accepted_line_starts):
            break
        if line.startswith("`"):
            split_line = line.split()
            column_name = split_line[0].strip("`").strip(",").lower()
            column_type = split_line[1].strip(",")
            table_columns.append(TableColumn(name=column_name, sql_data_type=column_type))
        elif line.startswith("constraint"):
            # Line structure:
            # CONSTRAINT `key_name` FOREIGN KEY (`column_name`) REFERENCES `other_table` (`other_table_column_name`)
            # Example:
            # CONSTRAINT `fk-test-tracker_code-tracker` FOREIGN KEY (`tracker_code`) REFERENCES `tracker` (`code`)
            _, column_name, other_table, other_table_column_name = re.findall(
                "constraint `([^`]+)` foreign key \(`([^`]+)`\) references `([^`]+)` \(`([^`]+)`\)", line
            )[0]
            foreign_keys.append(
                ForeignKeyData(
                    column_name=column_name,
                    referenced_table_name=other_table,
                    referenced_column_name=other_table_column_name,
                )
            )

    return TableData(table_name=table_name, table_columns=table_columns, foreign_keys=foreign_keys)


def read_dump_table_structure(dump_filename: str) -> list[TableData]:
    tables_data = []
    with open(dump_filename, "rb") as f:
        for i, line in enumerate(f):
            line = line.decode("utf-8").lower().strip()
            if line.startswith(f"create table"):
                table_name = line.split()[2].strip("`")
                tables_data.append(parse_table_structure(table_name, f))

    return tables_data


def read_dump_inserts(dump_filename: str, dump_structure: list[TableData]) -> dict[str, str]:
    tables_metadata = {
        table_data.table_name: [column.name for column in table_data.table_columns]
        for table_data in dump_structure
    }

    inserts_data: dict[str, str] = {}
    with open(dump_filename, "rb") as f:
        for i, line in enumerate(f):
            line = line.decode("utf-8").strip()
            if not line.startswith("INSERT INTO"):
                continue

            table_name = line.lower().split("`")[1]
            if table_name == "dependency_log":
                a = 3
            column_names = re.findall("\([^\)]*\)(?= VALUES)", line)
            if column_names:
                column_names = [name.strip(" ") for name in column_names[0].strip("() ").replace("`", "").split(",")]
            else:
                column_names = tables_metadata[table_name]

            insert_rows = [line_to_list_regex(x) for x in re.findall("(?<=VALUES) .*", line)[0].split("),(")]
            joined_insert_rows = [",".join(row) for row in insert_rows]
            column_names = [f"`{column_name}`" for column_name in column_names]
            data = f"INSERT INTO `{table_name}` ({','.join(column_names)}) VALUES ({'),('.join(joined_insert_rows)});"

            if inserts_data.get(table_name):
                data = re.sub(f"INSERT INTO `{table_name}` \([^\(\)]+\) VALUES", ",", data)
                inserts_data[table_name] = inserts_data[table_name].rstrip(";\n") + data
            else:
                inserts_data[table_name] = data

    return inserts_data


def _get_fks(
    table_name: str, columns_to_change: list[str], structure: list[TableData]
) -> dict[str, list[ForeignKeyReference]]:
    columns_fk_referenced: dict[str, list[ForeignKeyReference]] = {}
    for table_data in structure:
        for fk in table_data.foreign_keys:
            if fk.referenced_table_name == table_name and fk.referenced_column_name in columns_to_change:
                fk_reference = ForeignKeyReference(table_name=table_data.table_name, column_name=fk.column_name)
                if columns_fk_referenced.get(fk.referenced_column_name):
                    columns_fk_referenced[fk.referenced_column_name].append(fk_reference)
                else:
                    columns_fk_referenced[fk.referenced_column_name] = [fk_reference]

    return columns_fk_referenced


def get_insert_column_names(insert_line: str) -> list[str]:
    columns_str = re.findall("\([^\)]*\)(?= VALUES)", insert_line)
    return [name.strip(" ") for name in columns_str[0].strip("() ").replace("`", "").split(",")]


def anonymize(
    inserts_dict: dict[str, str], structure: list[TableData], table_columns_to_change: dict[str, list[str]]
) -> dict[str, str]:
    for table_name, columns_to_change in table_columns_to_change.items():
        columns_fk_referenced = _get_fks(table_name, columns_to_change, structure)
        insert_line = inserts_dict[table_name]
        column_names = get_insert_column_names(insert_line)


        column_names_and_indexes_to_change = [
            (column_name, column_names.index(column_name)) for column_name in columns_to_change
        ]
        new_line, changes = get_line_with_randomized_values(
            insert_line, table_name, column_names_and_indexes_to_change, column_names, columns_fk_referenced,
        )

        inserts_dict[table_name] = new_line
        inserts_dict = propagate_changes_in_fks(inserts_dict, columns_fk_referenced, changes)
    return inserts_dict


def get_line_with_randomized_values(
        line: str,
        table_name: str,
        column_names_and_indexes_to_change: list[tuple[str, int]],
        columns_in_insert_statement: list[str],
        columns_fk_referenced: dict[str, list[str]],
) -> tuple[str, dict[str, dict[Any, Any]]]:
    insert_rows = [line_to_list_regex(x) for x in re.findall("(?<=VALUES) .*", line)[0].split("),(")]
    faker = Faker()
    changes: dict[str, dict[Any, Any]] = {}

    for column_name, index in column_names_and_indexes_to_change:
        column_changes: dict[Any, Any] = {}
        for i, row in enumerate(insert_rows):
            old_value = row[index]
            row[index] = faker.lexify(f"'{table_name}-{column_name}-?????-{i + 1}'")
            if column_name in columns_fk_referenced and not column_changes.get(old_value):
                column_changes[old_value] = row[index]
            elif column_name in columns_fk_referenced:
                row[index] = column_changes[old_value]
        if column_name in columns_fk_referenced:
            changes[column_name] = column_changes

    joined_insert_rows = [",".join(row) for row in insert_rows]
    return (
        f"INSERT INTO `{table_name}` ({','.join(columns_in_insert_statement)}) VALUES ({'),('.join(joined_insert_rows)});",
        changes
    )


def propagate_changes_in_fks(
        inserts_dict: dict[str, str],
        columns_fk_referenced: dict[str, list[ForeignKeyReference]],
        changes: dict[str, dict[Any, Any]]
) -> dict[str, str]:
    for column_name, tables_reference in columns_fk_referenced.items():
        for table_reference in tables_reference:
            line = inserts_dict[table_reference.table_name]
            column_names = get_insert_column_names(line)
            column_index = column_names.index(table_reference.column_name)
            insert_rows = [line_to_list_regex(x) for x in re.findall("(?<=VALUES) .*", line)[0].split("),(")]
            for row in insert_rows:
                row[column_index] = changes[column_name][row[column_index]]

            joined_insert_rows = [",".join(row) for row in insert_rows]
            new_line = (
                f"INSERT INTO `{table_reference.table_name}` ({','.join(column_names)}) VALUES ({'),('.join(joined_insert_rows)});"
            )
            inserts_dict[table_reference.table_name] = new_line

    return inserts_dict


def write_in_file(dump_filename: str, lines: list[str]) -> None:
    output_filename = "out_dump.sql"
    output_lines = []
    seen_tables = set()

    with open(dump_filename, "rb") as f:
        for i, line in enumerate(f):
            line = line.decode("utf-8").strip()
            if not line.startswith("INSERT INTO"):
                output_lines.append(line + "\n")
            else:
                table_name = line.lower().split("`")[1]
                if table_name in seen_tables:
                    continue
                output_lines.append(lines[table_name] + "\n")
                seen_tables.add(table_name)

    with open(output_filename, "w") as out:
        out.writelines(output_lines)


if __name__ == "__main__":
    tables_structure = read_dump_table_structure("dump.sql")
    inserts = read_dump_inserts("dump.sql", tables_structure)
    inserts = anonymize(inserts, tables_structure, {"sample": ["code", "vial_code"], "test": ["code"]})

    write_in_file("dump.sql", inserts)
