from tb_tmp_daily_qsofa_log0 import TbTmpDailyqSOFALog0
from tb_tmp_daily_qsofa_log1 import TbTmpDailyqSOFALog1
from tb_tmp_daily_qsofa_log2 import TbTmpDailyqSOFALog2
from tb_tmp_daily_qsofa_log3 import TbTmpDailyqSOFALog3
from tb_tmp_daily_qsofa_log4 import TbTmpDailyqSOFALog4
from tb_tmp_daily_qsofa_log5 import TbTmpDailyqSOFALog5


class ETLDailyqSOFA():
    """ Creating daily DailyqSOFA table """

    def run(
        self,
    ):
        TbTmpDailyqSOFALog0().run()    # date
        TbTmpDailyqSOFALog1().run()    # RR(respiratory rate)
        TbTmpDailyqSOFALog2().run()    # BP(blood pressure)
        TbTmpDailyqSOFALog3().run()    # GCS(Glasgow coma scale)
        TbTmpDailyqSOFALog4().run()    # merge
        TbTmpDailyqSOFALog5().run()    # final


if __name__ == "__main__":

    obj = ETLDailyqSOFA()
    obj.run()

