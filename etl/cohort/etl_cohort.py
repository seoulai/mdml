from tb_tmp_cohort_log0 import TbTmpCohortLog0
from tb_tmp_cohort_log1 import TbTmpCohortLog1
from tb_tmp_cohort_log2 import TbTmpCohortLog2


class ETLCohort():
    """ Creating Cohort table """

    def run(
        self,
    ):
        TbTmpCohortLog0().run()    # prescriptions
        TbTmpCohortLog1().run()    # icu & abx
        TbTmpCohortLog2().run()    # suspected_infection


if __name__ == "__main__":

    obj = ETLCohort()
    obj.run()

