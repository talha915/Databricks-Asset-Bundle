import dlt

from src.common.config import (
    erp_folder,
    bronze_check_path
)

from src.services.ingestion_service import (
    IngestionService
)


class ERPBronze:

    def register(self):


        @dlt.table(
            name="bronze_erp"
        )
        def bronze_erp():

            return (
                IngestionService
                .read_csv_autoloader(
                    erp_folder,
                    bronze_check_path[
                        "schema_location"
                    ] + "/bronze_erp"
                )
            )