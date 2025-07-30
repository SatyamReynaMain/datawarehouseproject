Objective
Design and implement a modern data warehouse using SQL Server, integrating and transforming data from multiple business systems to enable insightful analytics and data-driven decisions.



Project Scope
Source Systems: Integrate data from two distinct business sources: ERP and CRM, both delivered as CSV files.

Data Quality: Address inconsistencies, duplicates, and formatting issues to ensure high-quality, reliable data.

Data Modeling: Combine source data into a unified, analysis-optimized model tailored for BI/reporting (no historization required).

Documentation: Deliver clear, accessible documentation for both business and technical audiences.


Data-warehouse-project/
│
├── datasets/                          # Raw CSV files extracted from ERP and CRM systems
│
├── docs/                              # Documentation and architectural design artifacts
│   ├── etl.drawio                    # Visual ETL design – processes, staging logic, and data paths
│   ├── data_architecture.drawio      # High-level system architecture of the warehouse
│   ├── data_catalog.md               # Metadata and field-level descriptions for each dataset
│   ├── data_flow.drawio              # Logical data flow diagram (from source to model)
│   ├── data_models.drawio            # Dimensional modeling visuals (e.g. star schema)
│   ├── naming-conventions.md         # Standards for naming tables, columns, and scripts
│
├── scripts/                           # SQL scripts categorized by transformation layer
│   ├── bronze/                        # Raw data ingestion scripts
│   ├── silver/                        # Cleansing, deduplication, and standardization logic
│   ├── gold/                          # Final analytical views and fact/dimension tables
│
├── tests/                             # Data quality checks and validation test cases
│
├── README.md                          # Getting started guide, project summary, and usage notes
├── LICENSE                            # Licensing terms and usage rights
├── .gitignore                         # Git exclusions for temp files, logs, etc.
└── requirements.txt                   # Environment setup and dependency definitions
