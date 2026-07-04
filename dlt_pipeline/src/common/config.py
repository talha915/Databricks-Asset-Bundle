# Configurations

source_catalog = "ai_lab_demo"
source_schema = "source"
source_volume = "source_volume"

# Folders
crm_folder = f"/Volumes/{source_catalog}/{source_schema}/{source_volume}/source_crm"
erp_folder = f"/Volumes/{source_catalog}/{source_schema}/{source_volume}/source_erp"

# File Paths
# crm_cust_info = f"{crm_folder}/cust_info.csv"
# crm_prd_info = f"{crm_folder}/prd_info.csv"
# crm_sales_details = f"{crm_folder}/sales_details.csv"

# erp_cust = f"{erp_folder}/CUST_AZ12.csv"
# erp_loc = f"{erp_folder}/LOC_A101.csv"
# erp_px_cat = f"{erp_folder}/PX_CAT_G1V2.csv"

source_file_map = {
    "crm_cust_info": f"{crm_folder}/cust_info.csv",
    "crm_prd_info": f"{crm_folder}/prd_info.csv",
    "crm_sales_details": f"{crm_folder}/sales_details.csv",

    "erp_cust": f"{erp_folder}/CUST_AZ12.csv",
    "erp_loc": f"{erp_folder}/LOC_A101.csv",
    "erp_px_cat": f"{erp_folder}/PX_CAT_G1V2.csv"
}


# Target
target_catalog = "ai_lab_demo"

target_schema_bronze = "bronze"
target_schema_silver = "silver"
target_schema_gold = "gold"


# Checkpoints
checkpoint_volume = "pipeline_checkpoints"

def get_check_paths(layer: str) -> dict:
    base = f"/Volumes/{target_catalog}/{layer}/{checkpoint_volume}"
    return {
        "checkpoint": f"{base}/checkpoints",
        "schema_checkpoint_path": f"{base}/schemas"
    }

bronze_check_path = get_check_paths(target_schema_bronze)
silver_check_path = get_check_paths(target_schema_silver)
gold_check_path = get_check_paths(target_schema_gold)