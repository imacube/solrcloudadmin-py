from configparser import ConfigParser, ExtendedInterpolation
import os

class ScriptsLibrary(object):
    def additional_arguments(self, parser, verbose=False):
        """
        Parse command line arguments.
        """
        parser.add_argument(
            '--profile', '-p', nargs=1, dest='profile', required=False,
            type=str,
            default=['DEFAULT'],
            help="""Profile from configuration file to use, defaults to DEFAULT"""
            )
        parser.add_argument(
            '--config', nargs=1, dest='config', required=False,
            type=str,
            default=['{}/.solrcloudadmin.config'.format(os.path.expanduser('~'))],
            help="""Configuration file to load"""
            )
        parser.add_argument(
            '--debug', action='store_true', required=False,
            help="""Turn on debug logging."""
            )

        if verbose:
            parser.add_argument(
                '--verbose', '-v', action='store_true', required=False,
                help="""Turn on verbose logging."""
                )
            
        return parser

    def load_configuation_files(self,
        general_configuration='.solrcloudadmin.config'):
        config = ConfigParser(interpolation=ExtendedInterpolation())
        if len(config.read(general_configuration)) == 0:
            print('Unable to load %s' % general_configuration)
            sys.exit(1)
        return config
