# MySQL Dump Anonymizer

Este projeto é uma ferramenta CLI para gerar um arquivo dump mysql com valores de colunas anonimizadas,
após ser fornecido um arquivo dump mysql como entrada.
Uso:
```
python mysql_dump_anonymizer/main.py <dump original> --target_file <arquivo dump de saída> --config_file <arquivo .json de configurações>
```
O script automaticamente propaga os valores alterados para foreign keys correspondentes. Desta forma, não se deve configurá-lo para alterar tanto uma coluna "mãe" de uma foreign key quanto uma "filha".

## Configuração inicial

Este projeto foi escrito em python +3.10 e utiliza o [Poetry](https://python-poetry.org/) para gerenciar suas dependências.
Consulte a [documentação oficial](https://python-poetry.org/docs/#installation) para instalá-lo se ainda não o fez.


### Ambiente virtual Python com Conda
1. Para que o Poetry não gerencie seus ambientes virtuais, configure-o para não criar novos ambientes virtuais
automaticamente (veja [docs](https://python-poetry.org/docs/faq/#i-dont-want-poetry-to-manage-my-virtual-environments-can-i-disable-it) para mais detalhes).
    ```sh
    poetry config virtualenvs.create false
    ```
1. Cirie um novo ambiente virtual com conda
    ```
    conda env create -f environment.yml
    ```
1. Ative o ambiente virtual
    ```
    conda activate dump-anon
    ```

### Intalação das dependências
    ```
    poetry install
    ```


## Arquivo de configuração
Para utilizar o script, é necessário um arquivo de configuração, para especificar que colunas devem ser alteradas.
Abaixo, um exemplo de arquivo de configuração.
```
{
  "tables": [
    {
      "table_name": "test",
      "columns_to_change": [
        {
          "name": "type_code",
          "subtype": "UUID",
          "regex": null
        }
      ]
    },
    {
      "table_name": "sample",
      "columns_to_change": [
        {
          "name": "code",
          "subtype": null,
          "regex": "^[0-9A-Za-z-]{6,30}$"
        },
        {
          "name": "vial_code",
          "subtype": null,
          "regex": "^[0-9A-Za-z-]{6,30}$"
        }
      ]
    }
  ]
}
```

É necessário especificar, para cada tabela, que colunas devem ser alteradas (se alguma). Os campos "subtype" e "regex" são opcionais. Atualmente, "subtype" só suporta "UUID" ou `null`. Quando fornecido, o script gerará valores UUID4 para aquele campo.
O campo "regex" é opcional, e quando fornecido, o script gerará valores aleatórios que obedecem àquele regex. Os regex devem ser simples (não conheço as limitações, mas são as limitações da biblioteca `rtsr`, que não as especifica em sua documentação).
