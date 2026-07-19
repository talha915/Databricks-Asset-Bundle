import importlib

from system.execution import start_run,end_run
from system.audit import write_audit
from framework.logger import log



def execute_task(layer,task_name):

    run_id=None

    try:

        run_id=start_run(
            layer,
            task_name
        )


        write_audit(
            run_id,
            "START",
            "Job started"
        )


        module = importlib.import_module(
            f"{layer}.{task_name}"
        )


        records = module.run()


        end_run(
            run_id,
            "SUCCESS",
            records
        )


        write_audit(
            run_id,
            "END",
            "Job completed"
        )


    except Exception as e:


        end_run(
            run_id,
            "FAILED",
            0,
            str(e)
        )


        write_audit(
            run_id,
            "ERROR",
            str(e)
        )

        raise e