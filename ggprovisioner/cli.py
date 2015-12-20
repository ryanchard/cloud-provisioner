import logging

from ggprovisioner import logger, Provisioner


def main():
    hdlr = logging.FileHandler('provisioner.log')

    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)

    prov = Provisioner()
    prov.run()

if __name__ == '__main__':
    main()
