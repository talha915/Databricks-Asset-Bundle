import dlt

from src.common.config import (
    source_file_map,
    bronze_check_path
)

from src.services.ingestion_service import (
    IngestionService
)


class CRMBronze:

    def register(self):


        @dlt.table(
            name="bronze_crm_customer"
        )
        def bronze_crm_customer():

            return (
                IngestionService
                .read_csv_autoloader(
                    source_file_map["crm_cust_info"],
                    bronze_check_path[
                        "schema_checkpoint_path"
                    ] + "/crm_customer"
                )
            )