import dlt

from src.common.config import (
    crm_folder,
    bronze_check_path
)

from src.services.ingestion_service import (
    IngestionService
)


class CRMBronze:

    def register(self):


        @dlt.table(
            name="bronze_crm"
        )
        def bronze_crm_customer():

            return (
                IngestionService
                .read_csv_autoloader(
                    crm_folder,
                    bronze_check_path[
                        "schema_location"
                    ] + "/bronze_crm"
                )
            )