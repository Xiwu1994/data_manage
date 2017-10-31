# -*- coding:utf-8 -*-
#读取配置信息的工具类

import os

class ConfigUtil(object):
    #初始化，从config/{env}/config.ini 读取配置文件内容
    def __init__(self,env='test'):
        file_abspath = os.path.abspath(__file__)
        bin_dir =  os.path.dirname(file_abspath)
        project_dir = os.path.dirname(bin_dir)
        config_dir = os.path.join(project_dir,'config',env)
        config_file = os.path.join(config_dir,'config.ini')
        self.load_config(config_file)

    def load_config(self,config_file):
        self.config = {}
        if not os.path.exists(config_file):
            raise IOError(config_file+" 不存在")
        config_file_handler = open(config_file)
        try:
            config_lines = config_file_handler.readlines()
            for config_line in config_lines:
                config_line_striped = config_line.strip()
                if config_line_striped <> '' and not config_line_striped.startswith("#"):
                    key_value_pair = config_line_striped.split("=")
                    if len(key_value_pair) == 2 :
                        key = key_value_pair[0].strip()
                        value = key_value_pair[1].strip()
                        self.config[key] = value
                    else:
                        print '读取 %s 出错' % (config_line_striped)
        finally:
            config_file_handler.close()

    def get_property_value(self,key):
        if not self.config.has_key(key):
            print '%s 不存在' % (key)
            return None
        else:
            return self.config.get(key)

    def get_all_mysql_server(self):
        mysql_server_list = {}
        for key,value in self.config.items():
            if key.startswith("metadata.mysql"):
                key_part_array = key.split(".")
                server = key_part_array[2]
                server_config_item = key_part_array[3]
                value = self.config[key]
                if server not in mysql_server_list:
                    server_config = {}
                    mysql_server_list[server] = server_config
                else:
                    server_config = mysql_server_list[server]
                server_config[server_config_item] = value

        return mysql_server_list








if __name__ == '__main__':
    configUtil = ConfigUtil(env="test")
    configUtil.get_all_mysql_server()


