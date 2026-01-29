# ******************************************************************************************************
#  main.py - Gbtc
#
#  Copyright © 2022, Grid Protection Alliance.  All Rights Reserved.
#
#  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
#  the NOTICE file distributed with this work for additional information regarding copyright ownership.
#  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
#  file except in compliance with the License. You may obtain a copy of the License at:
#
#      http://opensource.org/licenses/MIT
#
#  Unless agreed to in writing, the subject software distributed under the License is distributed on an
#  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
#  License for the specific language governing permissions and limitations.
#
#  Code Modification History:
#  ----------------------------------------------------------------------------------------------------
#  01/08/2024 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************

import argparse
from data_proxy import DataProxy
from process_data import process_data
from gsf import Limits
from typing import Final

MAXPORT: Final = Limits.MAXUINT16

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("hostname", type=str)
    parser.add_argument("wave_apps_port", type=int)
    parser.add_argument("data_pub_port", type=int)
    
    args = parser.parse_args()

    if args.wave_apps_port < 1 or args.wave_apps_port > MAXPORT:
        print(f"WaveApps port number \"{args.wave_apps_port}\" is out of range: must be 1 to {MAXPORT}")
        exit(2)

    if args.data_pub_port < 1 or args.data_pub_port > MAXPORT:
        print(f"Data publisher port number \"{args.data_pub_port}\" is out of range: must be 1 to {MAXPORT}")
        exit(2)

    data_proxy = DataProxy()
    
    # Set user defined callback function to handle time-aligned grouped data:
    data_proxy.set_groupeddata_receiver(process_data)

    try:
        # Start data proxy publisher and wait for connection from WaveApps host application
        data_proxy.start(args.data_pub_port)

        # We subscribe to all points that proxy is actively publishing -- configuration
        # of desired points is handled through WaveApps host application adapter:
        data_proxy.subscribe("FILTER ActiveMeasurements WHERE True", data_proxy.settings)
        data_proxy.connect(f"{args.hostname}:{args.wave_apps_port}", data_proxy.config)

        # Exit when enter key is pressed
        input()
    finally:
        data_proxy.dispose()

if __name__ == "__main__":
    main()
