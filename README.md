# Trabalho de Engenharia de Dados
Implementação de Apache Spark com Delta Lake e Apache Iceberg.

### Integrantes

- [Eliandra Cardoso](https://github.com/ardnaile)
- [Paulo Cesar Dal Ponte](https://github.com/pauloDalponte)

### Objetivo

Compreender o uso do Delta Lake e do Apache Iceberg ao manipular dados dentro do Jupyter Lab com o PySpark, com comandos de INSERT, UPDATE e DELETE.

### Referência

Além dos conhecimentos adquiridos em sala de aula, também foram utilizados como referência para o projeto a aula [Como Sair do Zero no Delta Lake e PySpark](https://youtu.be/eOrWEsZIfKU) da DataWay BR e o [artigo](https://datawaybr.medium.com/como-sair-do-zero-no-delta-lake-em-apenas-uma-aula-d152688a4cc8) referente a esta aula.

# Delta Lake

Abaixo está o passo a passo de como foi implementado o Delta Lake.

### 1. Configuração do ambiente

Para este projeto, foi utilizada uma máquina virtual Linux, já com o GIT instalado. Antes de iniciar o projeto, foram setados o user.name e user.email do GIT globalmente para criação do projeto pelo Poetry posteriormente:

`git config --global user.name "exemplo123"`

`git config --global user.email "exemplo123@exemplo.com"`

Instalamos o Python e o Poetry, que é um gerenciador de dependências e ferramenta de empacotamento para projetos Python. A instalação foi feita rodando os seguintes comandos:

Primeiro, como boa prática, atualizamos o gerenciador de pacotes do Linux:

`apt-get update` 

Instalamos o Python e o Poetry:

`apt-get install -y python3 python3-poetry`

Conferimos se a instalação foi bem sucedida: 

`python3 —version` 

`poetry --version`

### 2. Criação do projeto pelo Poetry

Criamos uma pasta para o projeto:

`mkdir <nome_da_pasta>`

Acessamos a pasta:

`cd <nome_da_pasta>`

Inicializamos um novo projeto com o Poetry dentro da pasta: 

`poetry init`

Instalamos as bibliotecas do PySpark, Delta Lake e Jupyter Lab por meio do Poetry:

`poetry add pyspark==3.4.2 delta-spark==2.4.0 jupyterlab`

Entramos no ambiente virtual criado pelo Poetry:

`poetry shell`

![ambiente-virtual-poetry](https://github.com/user-attachments/assets/7113e601-f423-42ab-972b-cc570607eec6)

Abrimos o Jupyter Lab

`jupyter-lab`

### 3. Configuração do projeto e prepraração dos dados no Jupyter Lab

Após rodar o comando `jupyter-lab`, o Jupyter Lab abre no navegador. Criamos uma pasta chamada "RAW" para simular a primeira camada onde são recebidos os dados no Data LakeHouse e um notebook chamado "delta" onde vamos trabalhar o código em si.

Importamos SparkSession e alguns tipos de dados que serão utilizados, e também o Delta:

```
# Imports

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType

from delta import *
```

Criamos a Spark Session:

```
spark = ( 
    SparkSession
    .builder
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate() 
)
```
Criamos um dataframe a partir de algums dados e do schema que criamos:

```
# Criando uma lista de tuplas que contem os dados da tabela cliente que vao ser utilizados neste caso

data_clientes = [
    ("1", "MARIA","12345678901"),
    ("2", "LUANA","99999999999"),
    ("3", "PAULO","11122233345")
]

# Criando o schema da tabela clientes

schema_clientes = (
    StructType([
        StructField("CLIENTE_ID", StringType(), False), # false indica que nao pode ser nulo
        StructField("NOME", StringType(), True),
        StructField("CPF", StringType(), True) # true indica que pode ser nulo
    ])
)

# Criando um dataframe a partir dos dados e do schema

df_clientes = spark.createDataFrame(data=data_clientes,schema=schema_clientes)

# Mostrando o dataframe criado

df_clientes.show(truncate=False)
```

### 4. Salvando o Delta Table e manipulando os dados nele

Salvamos o Delta Table localmente a partir do dataframe criado:

```
( 
    df_clientes
    .write
    .format("delta")
    .mode('overwrite')
    .save("./RAW/CLIENTES")
)
```

Simulamos a atualização de registros no Data Lakehouse:

```
new_data_clientes = [
    ("1", "MARIA LUIZA","12345678901")  
]

df_new_clientes = spark.createDataFrame(data=new_data_clientes, schema=schema_clientes)

df_new_clientes.show()
```

UPSERT:

```
deltaTable = DeltaTable.forPath(spark, "./RAW/CLIENTES")

(
    deltaTable.alias("dados_atuais")
    .merge(
        df_new_clientes.alias("novos_dados"),
        "dados_atuais.CLIENTE_ID = novos_dados.CLIENTE_ID"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

DELETE: 

```
deltaTable.delete("CPF = 12345678901")
```

Acessando o histórico de mudanças:

```
(
    deltaTable
    .history()
    .select("version", "timestamp", "operation", "operationMetrics")
    .show()
)
```

Lendo os dados da primeira versão da tabela:

```
(
    spark
    .read
    .format('delta')
    .option("versionAsOf", 0)
    .load('./RAW/CLIENTES')
    .show()
)
```

Restaurando a tabela para uma versão antiga, nesse caso a versão 1, antes de excluirmos o registro com id 1:

```
deltaTable.restoreToVersion(1)
```

Em todos os casos, este comando foi utilizado para conferir se o conteúdo da tabela foi alterado corretamente:

```
deltaTable.toDF().show()

```

# Apache Iceberg

Abaixo está o passo a passo de como foi implementado o Apache Iceberg.

### 1. Configuração do ambiente

Para este projeto, foi utilizada uma máquina virtual Linux, já com o GIT instalado. Antes de iniciar o projeto, setamos o user.name e o user.email do GIT globalmente para criação do projeto pelo Poetry posteriormente:

`git config --global user.name "exemplo123"`

`git config --global user.email "exemplo123@exemplo.com"`

Instalamos o Python e o Poetry, que é um gerenciador de dependências e ferramenta de empacotamento para projetos Python. A instalação foi feita rodando os seguintes comandos:

Primeiro, como boa prática, atualizamos o gerenciador de pacotes do Linux:

`apt-get update` 

Instalamos o Python e o Poetry:

`apt-get install -y python3 python3-poetry`

Conferimos se a instalação foi bem sucedida: 

`python3 —version` 

`poetry --version`

### 2. Criação do proj eto pelo Poetry

Criamos uma pasta para o projeto:

`mkdir <nome_da_pasta>`

Acessamos a pasta:

`cd <nome_da_pasta>`

Inicializamos um novo projeto com o Poetry: 

`poetry init`

Instalamos as bibliotecas do PySpark, Iceberg e Jupyter Lab por meio do Poetry:

`poetry add pyspark jupyterlab`

Baixar o arquivo do Iceberg:

`wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark3-runtime/1.3.0/iceberg-spark3-runtime-1.3.0.jar`

Para verificarmos as versões do PySpark e Python do projeto podemos acessar o arquivo pyProject.poml:

`nano pyproject.poml`

Para verificarmos versão do Jupyter:

`jupyter --version`

Entramos no ambiente virtual criado pelo Poetry:

`poetry shell`

Abrimos o Jupyter Lab

`jupyter-lab`

### 3. Configuração do projeto e prepraração dos dados no Jupyter Lab

Após rodar o comando `jupyter-lab`, o Jupyter Lab abre no navegador. Criamos um notebook chamado "iceberg" onde vamos trabalhar o código em si.

Importamos SparkSession e alguns tipos de dados que serão utilizados, e também o Delta:


```
# Imports

import os
from pyspark.sql import SparkSession
```

Criamos a Spark Session:

```

iceberg_jar_path = os.path.join(os.getcwd(), "<caminho/ate/pasta/jar/do/iceberg>/

spark = SparkSession.builder \
    .appName("IcebergApp") \
    .config("spark.jars", iceberg_jar_path) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "in-memory") \
    .getOrCreate()
```

### 4. Criando a tabela e manipulando a mesma

Criação da Tabela

```
spark.sql("""
CREATE TABLE my_catalog.default.my_table (
    id INT,
    name STRING,
    cpf STRING,
    created_at TIMESTAMP
) 
USING iceberg
""")
```

Simulamos a inserção de dados:

```
spark.sql("""
INSERT INTO my_catalog.default.my_table VALUES
    (1, 'Ana','11526558950', current_timestamp()),
    (2, 'Paulo,'11520939914', current_timestamp())
""")
```

Consulta:

```
result = spark.sql("SELECT * FROM my_catalog.default.my_table")
result.show()
```

Update: 

```
spark.sql("""
UPDATE my_catalog.default.my_table
SET cpf = '54689578510'
WHERE id = 1
""")
```

Delete: 

```
spark.sql("""
DELETE FROM my_catalog.default.my_table
WHERE id = 2
""")
```

