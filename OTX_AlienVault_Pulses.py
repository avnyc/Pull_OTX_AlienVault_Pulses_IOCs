import datetime
from file_locations import alienvault_logger, pulse_output_file, otx_indicators_and_types
from OTXv2 import OTXv2
import pandas as pd
import time

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('expand_frame_repr', False)

# Start timer
start = time.time()


class alienvault_otx():
    alienvault_base_url = 'https://otx.alienvault.com/api/v1/pulses/subscribed'

    def __init__(self, otx_api, days_to_go_back):
        self.otx_api = otx_api
        self.days_to_go_back = days_to_go_back
        alienvault_logger.info(f'Number of days back are {self.days_to_go_back}')
        self.days_back, self.today_ = self.days_back_to_pull()
        self.otx_retrieve_all_pulses()

    def days_back_to_pull(self):
        try:
            # Pull data
            today = datetime.datetime.today()
            today_ = today
            today_ = today_.strftime('%Y-%m-%d')
            today = today - datetime.timedelta(days=self.days_to_go_back)
            days_back = today.strftime('%Y-%m-%d')

            # Log information
            alienvault_logger.info(f'We are pulling data from {days_back} till {today_}')

            # Return values
            return days_back, today_

        except Exception as e:
            alienvault_logger.info(f'Failed to pull days back variable.')

    def otx_retrieve_all_pulses(self):
        try:
            """
            Retrieve All pulses from AlienVault
            :return: pandas dataframe saved as an Excel file containing all AlientVault pulses
            """

            # OTX class
            otx = OTXv2(self.otx_api)

            # Retrieve the url
            current_pulse = otx.get('https://otx.alienvault.com/api/v1/pulses/subscribed', limit=10,
                                    modified_since=self.days_back)

            pulses_results = current_pulse['results']

            # Create first dataframe to append to
            pulse_df = pd.DataFrame.from_dict(pulses_results)

            while 'next' in current_pulse:
                # Retrieve the next API link
                next_pulse_df = pd.DataFrame.from_dict(current_pulse)
                next_pulse = next_pulse_df['next'][0]

                # Make sure link is not None
                if next_pulse is not None:

                    # Retrieve data from the current pulse
                    current_pulse = otx.get(next_pulse)

                    # Slice down to the results only
                    pulses_results = current_pulse['results']

                    # Convert current data into df
                    next_pulse_df = pd.DataFrame.from_dict(pulses_results)

                    # Append current df to base df
                    pulse_df = pd.concat([pulse_df, next_pulse_df], ignore_index=True, sort=False)
                    alienvault_logger.info(f'The shape of the pulse df is {pulse_df.shape[0]}')

                    # Delete next pulse df
                    del next_pulse_df

                else:
                    alienvault_logger.info(f'The total shape of all Pulses from AlientVault is {pulse_df.shape[0]}')

                    # Save dataframe(df) to file location previously named above.
                    pulse_df.to_csv(pulse_output_file, index=False)
                    alienvault_logger.info(f'The pulse_df file has been saved!')
                    break

            """
            Convert All pulses from AlienVault into a csv file containing indicator and type
            :return: pandas dataframe saved as an Excel file containing all SHA-256 files, IP addresses, urls, 
            and domains
            """
            df = pulse_df.copy(deep=True)
            alienvault_logger.info(f'\n')
            alienvault_logger.info(f'{df.head(1)}')
            alienvault_logger.info(f'\n')

            alienvault_logger.info(f'Successfully imported pulse df.')

            granular_otx_df = None
            alienvault_logger.info(f'The size of the df is {df.shape[0]}')
            # Enumerate df
            for index, row in enumerate(df.itertuples(index=False)):

                # Retrieve indicator column and turn column into its own df
                indicator = row.indicators
                temp_granular_df = pd.DataFrame(indicator)

                # Add name of OTX pulse into 'name' column of new df
                temp_granular_df['name'] = row.name

                # Populate granular df
                if granular_otx_df is None:
                    granular_otx_df = temp_granular_df.copy(deep=True)
                else:
                    granular_otx_df = pd.concat([granular_otx_df, temp_granular_df], ignore_index=True, sort=False)
                    alienvault_logger.info(f'The row is {index} and total rows in the otx_df are '
                                           f'{granular_otx_df.shape[0]}')

            del temp_granular_df

            granular_otx_df.to_csv(otx_indicators_and_types, index=False)
            alienvault_logger.info(f'The otx_df file has been saved!')
            return granular_otx_df

        except Exception as e:
            alienvault_logger.error(f'Error is {e}')


if __name__ == '__main__':
    try:
        # This is very important. This is where you insert your API key and tell OTX how many days back you want to pull
        # their pulses.
        alienvault_otx(otx_api='',
                       days_to_go_back=7)
        end = time.time()
        alienvault_logger.info(f'\n')
        alienvault_logger.info(f'No errors! The total execution time is {end - start} seconds.')
    except Exception as e:
        # Python Error Code
        alienvault_logger.info(f'The code failed. The error is {e}.')
