import dlt

from src.common.config import (
    source_file_map,
    bronze_check_path
)

from src.services.ingestion_service import (
    IngestionService
)


class ERPBronze:

    def register(self):


        @dlt.table(
            name="bronze_erp_customer"
        )
        def bronze_erp_customer():

            return (
                IngestionService
                .read_csv_autoloader(
                    source_file_map["erp_cust"],
                    bronze_check_path[
                        "schema_checkpoint_path"
                    ] + "/erp_customer"
                )
            )