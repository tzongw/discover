# -*- coding: utf-8 -*-
from config import define, options
import logging

define('name', '', str, 'name')


def main():
    logging.info(f'entry {options.name}')
