# Juntos Somos Mais • Data Analytics Engineer

## Contextualização 

O objetivo desse case é criar um desenho de solução, com passo a passo para orientar um engenheiro de dados desenvolver uma solução de dados real time para a squad de pedidos. O resultado desse desenho deverá orientar as pessoas de dados e da squad com a solução. Esses documentos servirão como insumo para iniciar o desenvolvimento.

A squad de produtos hoje conta com um(a) PM e pessoas de engenharia de software. Esse time fará parte da solução, criando o transacional da nossa solução de dados, portanto é necessário desenharmos a solução que contemple eles. Aqui é necessário sugerir uma arquitetura e ferramentas para a Squad, contextualizando tecnicamente e funcionalmente o time. A stack do lado de dados conta com um Databricks na Azure, mas ainda não tem uma solução para dados real time.

Os dados que receberemos estão nesse modelo ([https://www.kaggle.com/datasets/gabrielramos87/an-online-shop-business](https://na01.safelinks.protection.outlook.com/?url=https%3A%2F%2Fwww.kaggle.com%2Fdatasets%2Fgabrielramos87%2Fan-online-shop-business&data=05%7C02%7C%7C6e0b23fb84bd4028e41808dd93271da7%7C84df9e7fe9f640afb435aaaaaaaaaaaa%7C1%7C0%7C638828518409908245%7CUnknown%7CTWFpbGZsb3d8eyJFbXB0eU1hcGkiOnRydWUsIlYiOiIwLjAuMDAwMCIsIlAiOiJXaW4zMiIsIkFOIjoiTWFpbCIsIldUIjoyfQ%3D%3D%7C0%7C%7C%7C&sdata=sXKtMvaEqs9kLbCquxEm%2BLPzoF0PxOGG8dzFKv8YZb0%3D&reserved=0))

É necessário também desenvolver um trecho de código para servir de exemplo para os engenheiros de software e de dados. Aqui, deixamos a sua escolha qual parte do código deseja construir para servir de exemplo. Podemos utilizar, Python, Pyspark, Sql e/ou Scala. Precisamos também explicar os motivadores para a escolha de tecnologia. Para documentar essa solução utilize uma ferramenta como [Draw.io](https://na01.safelinks.protection.outlook.com/?url=http%3A%2F%2Fdraw.io%2F&data=05%7C02%7C%7C6e0b23fb84bd4028e41808dd93271da7%7C84df9e7fe9f640afb435aaaaaaaaaaaa%7C1%7C0%7C638828518409922196%7CUnknown%7CTWFpbGZsb3d8eyJFbXB0eU1hcGkiOnRydWUsIlYiOiIwLjAuMDAwMCIsIlAiOiJXaW4zMiIsIkFOIjoiTWFpbCIsIldUIjoyfQ%3D%3D%7C0%7C%7C%7C&sdata=Mz5%2BxTPt%2FZ9K4q3Vv4HzNPjFPx99XC2Bx0bYx4TUOX0%3D&reserved=0), Miro ou excalidraw e para o código, utilize o github.

## Solução estratégica

### Visão Geral

A solução proposta segue o seguinte fluxo:
1. Eventos de pedidos gerados pela aplicação transacional são enviados em tempo real ao Confluent Kafka.  
2. Do Kafka, os dados são consumidos e armazenados no Azure Blob Storage no formato Delta Lake. 
3. O Databricks acessa os dados armazenados e realiza processamento em camadas (Medalhão: Bronze, Silver e Gold).
4. Finalmente, os dados são disponibilizados no Metabase para visualização pelos usuários finais.
   

![juntos_somos_mais drawio](https://github.com/user-attachments/assets/de5a74eb-7fab-4648-b240-a280d72b0e10)


### Tecnologias

#### Ingestão

O **Confluent Kafka** é uma plataforma de streaming de eventos baseada no **Apache Kafka**, projetada para ingestão e processamento de dados em tempo real. Além das funcionalidades do Kafka open-source, a versão da Confluent oferece recursos adicionais como gerenciamento simplificado, segurança aprimorada, conectores prontos e integração com nuvens como Azure. Assim sendo, nesse projeto, Confluent Kakfa, atuará como ponte entre o sistema transacional da squad e o pipeline de dados. 

**Por que utilizar o Confluent Kafka nesta etapa?**

- **Baixa latência e alta performance:** Ideal para capturar eventos (como pedidos) em tempo real, com alta confiabilidade e tolerância a falhas;
- **Conectores prontos:** Facilita a integração com bancos de dados, APIs e ferramentas como Databricks;
- **Gerenciamento simplificado:** Interface para monitoramento, controle de schema (Schema Registry) e governança;
- **Escalabilidade:** Suporta grandes volumes de dados com crescimento horizontal;
- **Segurança:** Suporte a autenticação, autorização e criptografia;
- **Integração com Azure e Databricks:** Fácil conexão com a stack existente da empresa.

#### Armazenamento

Considerando a infraestrutura já existente na Azure, opta-se pelo **Azure Storage Account** para armazenar os dados recebidos em tempo real do Kafka. Essa é uma solução de armazenamento escalável e segura da Microsoft, que oferece diferentes tipos de armazenamento para atender a diversas necessidades.

Para este projeto, utilizaremos especificamente o **Blob Storage** (Binary Large Object), pois é a opção mais adequada para lidar com grandes volumes de dados armazenados em arquivos que serão gerados continuamente pelo pipeline em tempo real. O Blob Storage é otimizado para esse tipo de cenário, garantindo desempenho e flexibilidade no consumo dos dados.

**Por que utilizar o Azure Storage Account nesta etapa?**

- **Integração nativa com Databricks:** Permite leitura e escrita eficientes, com suporte direto ao formato **Delta Lake**, o que otimiza consultas e facilita atualizações incrementais;
- **Escalabilidade e custo-benefício:** Suporta grandes volumes de dados com custo competitivo, ideal para ingestões contínuas e crescentes;
- **Estrutura em camadas (Bronze/Silver/Gold):** Facilita a organização dos dados dentro de uma arquitetura **lakehouse**, separando dados brutos, limpos e transformados;
- **Alta disponibilidade e segurança:** Oferece redundância geográfica, criptografia em repouso e integração com o Azure Active Directory para controle de acesso;
- **Desacoplamento dos sistemas:** Armazena os dados de forma independente do Kafka, permitindo que diferentes áreas (engenharia, BI, ciência de dados) acessem os dados sem impacto na ingestão.

#### Processamento  

Após armazenarmos os dados brutos no Azure Storage, utilizaremos o **Databricks** para processá-los e aplicar a **Arquitetura em Camadas (Medalhão)** — um padrão amplamente adotado para organizar dados em pipelines analíticos.

A Arquitetura Medalhão é composta por três camadas principais:

- **Bronze** (dados brutos),
- **Silver** (dados limpos e estruturados),
- **Gold** (dados agregados e prontos para análise).

Essa abordagem garante maior organização e governança dos dados, além de facilitar a manutenção, reprocessamentos e auditorias. Também assegura **princípios ACID** (Atomicidade, Consistência, Isolamento e Durabilidade) com uso de **Delta Lake**, permitindo confiabilidade e versionamento dos dados ao longo das transformações.

**Por que utilizar o Databricks neste etapa?**
- **Integração nativa com Azure e Blob Storage:** Permite acesso otimizado aos dados com segurança e performance;
- **Suporte nativo ao Delta Lake:** Facilita a aplicação da arquitetura Bronze → Silver → Gold, com controle de versões e updates transacionais;
- **Ambiente colaborativo:** Possibilita que engenheiros de dados, analistas e cientistas trabalhem juntos em notebooks com Python, SQL, PySpark e Scala;
- **Escalabilidade e performance:** Ideal para processar grandes volumes de dados com clusters elásticos e paralelismo automático.

#### Visualização e consumo dos dados

Para a camada de consumo dos dados, optamos pelo **Metabase** como ferramenta de visualização. Trata-se de uma plataforma **open source**, de fácil implantação e uso, que se destaca pela **intuitividade** e **flexibilidade** no acesso e exploração de dados.

**Por que escolher o Metabase nesta etapa?**

- **Fácil integração com o Databricks**;
- **Curva de aprendizado baixa**, o que permite adoção rápida por toda a squad, inclusive pessoas sem background técnico;
- **Código aberto e gratuito**, com possibilidade de personalizações ou uso da versão Enterprise conforme a maturidade da empresa;
- **Foco na autonomia dos times de produto e negócio**, reduzindo a dependência do time de dados para análises simples e operacionais;
- **Suporte a filtros dinâmicos e segmentações**, permitindo que os dashboards atendam a diferentes públicos com o mesmo conjunto de dados.
