# Configurations

source_catalog = "ai_lab_demo"
source_schema = "source"
source_volume = "source_volume"

# Folders
crm_folder = f"/Volumes/{source_catalog}/{source_schema}/{source_volume}/source_crm"
erp_folder = f"/Volumes/{source_catalog}/{source_schema}/{source_volume}/source_erp"

# File Paths
crm_cust_info = f"{crm_folder}/cust_info.csv"
crm_prd_info = f"{crm_folder}/prd_info.csv"
crm_sales_details = f"{crm_folder}/sales_details.csv"

erp_cust = f"{erp_folder}/CUST_AZ12.csv"
erp_loc = f"{erp_folder}/LOC_A101.csv"
erp_px_cat = f"{erp_folder}/PX_CAT_G1V2.csv"
