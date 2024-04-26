import re
import time
from typing import Any, Tuple

from faker import Faker
from models import ForeignKeyData, TableColumn, TableData

time_char_by_char = 0
time_regex = 0

time_total = 0
time_update_changes_dict = 0


def line_to_list(line: str) -> list[str]:
    global time_char_by_char
    begin = time.time()
    clean_line = line.strip("()").replace("`", "")
    ret = []
    quote_mark = 0
    field = ""
    for c in clean_line:
        if c == "," and not quote_mark % 2:
            ret.append(field)
            field = ""
        elif c == "'":
            quote_mark += 1
        else:
            field += c
    time_char_by_char += time.time() - begin

    return ret + [field]


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


def read_dump_table_structure(dump_filename) -> list[TableData]:
    tables_data = []
    with open(dump_filename, "rb") as f:
        for i, line in enumerate(f):
            line = line.decode("utf-8").lower().strip()
            if not line.startswith(f"create table"):
                continue
            current_table_name = line.split()[2].strip("`")
            tables_data.append(parse_table_structure(current_table_name, f))
    return tables_data


def write_insert_file(
    dump_filename: str, table_columns_to_change: dict[str, list[str]]
) -> dict[str, dict[str, dict[Any, Any]]]:
    tables_metadata = {
        table_data.table_name: [column.name for column in table_data.table_columns]
        for table_data in read_dump_table_structure("dump.sql")
    }
    output_filename = "out_dump.sql"
    output_lines = []

    with open(dump_filename, "rb") as f:
        for line in f:
            line = line.strip().decode("utf-8")
            insert_line_prefix = "insert into"
            if not line.lower().startswith(insert_line_prefix):
                output_lines.append(line + "\n")
                continue

            table_name = line.lower().split("`")[1]
            column_names = re.findall("\([^\)]*\)(?= VALUES)", line)
            insert_contains_column_names = False
            if column_names:
                column_names = [name.strip(" ") for name in column_names[0].strip("() ").replace("`", "").split(",")]
                insert_contains_column_names = True
            else:
                column_names = tables_metadata[table_name]
            columns_to_change = table_columns_to_change.get(table_name)
            if columns_to_change:
                column_names_and_indexes_to_change = [
                    (column_name, column_names.index(column_name)) for column_name in columns_to_change
                ]
                line, changes = get_line_with_randomized_values(
                    line,
                    table_name,
                    column_names_and_indexes_to_change,
                    columns_in_insert_statement=column_names if insert_contains_column_names else None,
                )
            output_lines.append(line + "\n")

    with open(output_filename, "w") as out:
        out.writelines(output_lines)

    if changes:
        return changes


def _update_changes_dict(
    changes: dict[str, dict[str, dict[Any, Any]]],
    table_name: str,
    column_name: str,
    row: list[str],
    index: int,
    i: int,
) -> dict[str, dict[str, dict[Any, Any]]]:
    global time_update_changes_dict
    begin_ = time.time()
    if not changes[table_name].get(column_name):
        changes[table_name][column_name] = {}

    old_value = row[index]
    if changes[table_name][column_name].get(old_value):
        return changes

    faker = Faker()
    changes[table_name][column_name].update({old_value: faker.lexify(f"'{table_name}-{column_name}-?????-{i + 1}'")})
    time_update_changes_dict += time.time() - begin_
    return changes


def get_line_with_randomized_values(
    line: str,
    table_name: str,
    column_names_and_indexes_to_change: list[tuple[str, int]],
    columns_in_insert_statement: list[str] | None = None,
) -> tuple[str, dict[str, dict[str, dict[str, dict[str, dict[Any, Any]]]]]]:
    insert_rows = [line_to_list_regex(x) for x in re.findall("(?<=VALUES) .*", line)[0].split("),(")]
    changes: dict[str, dict[str, dict[Any, Any]]] = {}
    changes[table_name] = {}

    for i, row in enumerate(insert_rows):
        for column_name, index in column_names_and_indexes_to_change:
            changes = _update_changes_dict(changes, table_name, column_name, row, index, i)
            row[index] = changes[table_name][column_name][row[index]]

    joined_insert_rows = [",".join(row) for row in insert_rows]
    if not columns_in_insert_statement:
        return f"INSERT INTO `{table_name}` VALUES ({'),('.join(joined_insert_rows)});", changes
    return (
        f"INSERT INTO `{table_name}` ({','.join(columns_in_insert_statement)}) VALUES ({'),('.join(joined_insert_rows)});",
        changes,
    )


if __name__ == "__main__":
    begin = time.time()
    tables_structure = read_dump_table_structure("dump.sql")
    changes_ = write_insert_file(
        "dump.sql", table_columns_to_change={"test": ["type_name"], "sample": ["code", "volume"]}
    )
    changes_.update(write_insert_file("dump.sql", table_columns_to_change={"sample": ["code"]}))
    time_total = time.time() - begin
    print(f"{time_total=}")
    print(f"{time_update_changes_dict=}")
    print(f"Ratio: {time_update_changes_dict/time_total}")

# associar o valor alterado com o valor antigo, para poder propagar as mudanças nas outras tabelas: valor antigo -> valor alterado (determinístico)
# não fazer dicionários aninhados (evitar ao máximo)
# ideia: não salvar no dict a informação da tabela sendo alterada e nem da coluna. simplesmente fazer algum dict como dict["valor antigo"] = "valor alterado"
# ideia melhor: alterar uma tabela de cada vez (das que o usuário pediu para alterar). Depois de alterar uma coluna dessa tabela que serve de foreign key para outra tabela, alteramos todas as colunas das tabelas que referenciam aquela foreign key.
