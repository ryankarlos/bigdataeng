from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import Athena, Glue, GlueCrawlers, GlueDataCatalog
from diagrams.aws.general import User
from diagrams.aws.integration import SNS, StepFunctions
from diagrams.aws.storage import SimpleStorageServiceS3

with Diagram("AWS Serverless Spark Workflow", show=True, direction="LR"):
    User1 = User("User")
    User2 = User("User")
    StepFunction = StepFunctions("Step Function Job")
    with Cluster("State Machine"):
        Glue_Job = Glue("Glue ETL Job")
        Crawler = GlueCrawlers("GlueCrawler")
        SNS_Email = SNS("SNS Email")
    Data_Catalog = GlueDataCatalog("GlueDataCatalog")
    S31 = SimpleStorageServiceS3("TSV Data")
    S32 = SimpleStorageServiceS3("Parquet Data")
    Athena = Athena("Result Queries")
    StepFunction << Edge(label="Trigger Step Function") << User1
    StepFunction >> Glue_Job
    StepFunction >> Crawler
    StepFunction >> SNS_Email
    S31 >> Glue_Job >> S32 >> Crawler
    Glue_Job >> Crawler >> SNS_Email >> User
    Crawler >> Data_Catalog
    Data_Catalog - Athena >> User2
