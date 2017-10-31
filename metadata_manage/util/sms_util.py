# -*- coding:utf-8 -*-
#异常信息报警发送的相关类
from config_util import ConfigUtil
import urllib2
import json
import  sys
class SmsUtil(object):
    def __init__(self,env='test'):
        self.config_util = ConfigUtil(env)
        self.sms_url = self.config_util.get_property_value('sms.url')
        self.sms_receivers = self.config_util.get_property_value('sms.receivers')



    def send_msg(self,message):
        # 新版的接口需要的参数：['from'=>7100,'user_id'=>100 ,'mobiles' => $username, "template" => "line.reserver_register", 'data' => json_encode(["pwd" => $pwd])];
        # 模版的定义需要添加到：http://dev.xunhuji.me:17990/projects/BEEPER/repos/beeper_sms/browse/config/template
        send_data = {
            "mobile":self.sms_receivers,
            "template":"super_template",
            "data":{
                "content":message
            }
        }
        request_url = "http://"+self.sms_url+"/api/v1/sms/send/template"
        print request_url
        request = urllib2.Request(url=request_url,data=json.dumps(send_data))
        request.add_header('Content-Type','application/json')
        response = urllib2.urlopen(request)
        response_data = response.read()
        print response_data
        response.close()


if __name__ == '__main__':
    env = 'test'
    if(len(sys.argv) == 2):
        env = sys.argv[1]
    print env
    sms_util = SmsUtil(env)
    message = 'test sms'
    sms_util.send_msg(message)


