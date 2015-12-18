# -*- coding: utf-8 -*-
from frontera.settings import BaseSettings, default_settings as frontera_settings
from distributed_frontera.settings import default_settings


class Settings(BaseSettings):
    def __init__(self, module=None, attributes=None):
        super(Settings, self).__init__(frontera_settings)
        self.add_module(default_settings)
        if module:
            self.add_module(module)
        if attributes:
            self.set_from_dict(attributes)