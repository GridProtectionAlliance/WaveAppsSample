# ******************************************************************************************************
#  data_proxy_publisher.py - Gbtc
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
#  07/30/2024 - J. Ritchie Carroll
#       Generated original version of source code.
#
# ******************************************************************************************************
# pyright: reportAttributeAccessIssue=false

import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from random import random
from threading import Timer
from typing import List
from uuid import UUID, uuid4

import numpy as np

# --- compiled/installed STTP Python API -------------------------------------
from sttp.publisher import Publisher
from sttp.data.dataset import DataSet
from sttp.data.datatype import DataType
from sttp.metadata.signaltype import SignalType
from sttp.metadata.signalreference import SignalReference, SignalKind
from sttp.transport.measurement import Measurement
from sttp.transport.subscriberconnection import SubscriberConnection
from sttp.ticks import Ticks

@dataclass
class DeviceDef:
    acronym: str
    name: str
    company: str = "GPA"
    protocol: str = "STTP"
    frames_per_second: int = 30
    longitude: float = 0.0
    latitude: float = 0.0
    unique_id: UUID = field(default_factory=uuid4)


@dataclass
class MeasurementDef:
    point_tag: str
    description: str
    signal_type: SignalType = SignalType.CALC
    source_index: int | None = None          # phasor source index, when applicable
    signal_id: UUID = field(default_factory=uuid4)


@dataclass
class PhasorDef:
    label: str
    phasor_type: str                          # "V" (voltage) or "I" (current)
    phase: str                                # "+", "A", "B", ...
    source_index: int

class DataProxy_Publisher(Publisher):
    """
    A Publisher whose metadata comes from in-code definitions instead of an XML
    file. Assign `device`, `measurements`, and `phasors`, then call
    `load_metadata()` (in place of setting metadata_path + the XML load); this
    override builds the DataSet from code and defines it directly.
    """
    
    def __init__(self):
        super().__init__()
        self.device: DeviceDef | None = None
        self.measurements: List[MeasurementDef] = []
        self.phasors: List[PhasorDef] = []

    # --- Neutralize the XML-based API so it can't be used by accident --------
    def load_metadata(self) -> Exception | None:
        # Override: build the DataSet from code and define it directly.
        try:
            self.define_metadata(self._build_dataset())
            return None
        except Exception as ex:  # mirror base-class error-return contract
            return ex

    # --- Build the exact DataSet shape define_metadata()/filter expect -------
    def _build_dataset(self) -> DataSet:
        if self.device is None:
            raise ValueError("device metadata has not been defined")

        now = _sttp_timestamp()
        dataset = DataSet("NewDataSet")

        self._build_device_detail(dataset, now)
        self._build_measurement_detail(dataset, now)
        self._build_phasor_detail(dataset, now)

        return dataset

    def _build_device_detail(self, dataset: DataSet, now: str) -> None:
        # Columns/types mirror MetadataTemplate.xml's DeviceDetail schema; only
        # the columns define_metadata() actually reads are strictly required,
        # but we include the common set for parity.
        table = dataset.create_table("DeviceDetail")
        _add_columns(table, [
            ("UniqueID", DataType.GUID),
            ("Acronym", DataType.STRING),
            ("Name", DataType.STRING),
            ("ProtocolName", DataType.STRING),
            ("FramesPerSecond", DataType.INT32),
            ("CompanyAcronym", DataType.STRING),
            ("Longitude", DataType.DECIMAL),
            ("Latitude", DataType.DECIMAL),
            ("Enabled", DataType.BOOLEAN),
            ("UpdatedOn", DataType.DATETIME),
        ])
        dataset.add_table(table)

        d = self.device
        row = table.create_row()
        row["UniqueID"] = d.unique_id
        row["Acronym"] = d.acronym
        row["Name"] = d.name
        row["ProtocolName"] = d.protocol
        row["FramesPerSecond"] = np.int32(d.frames_per_second)
        row["CompanyAcronym"] = d.company
        row["Longitude"] = Decimal(str(d.longitude))
        row["Latitude"] = Decimal(str(d.latitude))
        row["Enabled"] = True
        row["UpdatedOn"] = now
        table.add_row(row)

    def _build_measurement_detail(self, dataset: DataSet, now: str) -> None:
        # define_metadata() and filter_metadata() read every one of these by
        # name, so all must be present.
        table = dataset.create_table("MeasurementDetail")
        _add_columns(table, [
            ("DeviceAcronym", DataType.STRING),
            ("ID", DataType.STRING),
            ("SignalID", DataType.GUID),
            ("PointTag", DataType.STRING),
            ("SignalReference", DataType.STRING),
            ("SignalAcronym", DataType.STRING),
            ("PhasorSourceIndex", DataType.INT32),
            ("Description", DataType.STRING),
            ("Internal", DataType.BOOLEAN),
            ("Enabled", DataType.BOOLEAN),
            ("UpdatedOn", DataType.DATETIME),
        ])
        dataset.add_table(table)

        device_acronym = self.device.acronym

        for index, m in enumerate(self.measurements, start=1):
            acronym = m.signal_type.acronym if m.signal_type != SignalType.UNKN else "CALC"
            kind = m.signal_type.signalkind

            if m.signal_type != SignalType.UNKN:
                signal_reference = SignalReference.tostring(acronym, kind, m.source_index)
            else:
                signal_reference = f"{device_acronym}-{acronym}"

            row = table.create_row()
            row["DeviceAcronym"] = device_acronym
            row["ID"] = f"{device_acronym}:{index}"     # "Source:ID" form filter_metadata parses
            row["SignalID"] = m.signal_id
            row["PointTag"] = m.point_tag
            row["SignalReference"] = signal_reference
            row["SignalAcronym"] = acronym
            row["PhasorSourceIndex"] = np.int32(m.source_index or 0)
            row["Description"] = m.description
            row["Internal"] = True
            row["Enabled"] = True
            row["UpdatedOn"] = now
            table.add_row(row)

    def _build_phasor_detail(self, dataset: DataSet, now: str) -> None:
        if not self.phasors:
            return

        table = dataset.create_table("PhasorDetail")
        _add_columns(table, [
            ("ID", DataType.INT32),
            ("DeviceAcronym", DataType.STRING),
            ("Label", DataType.STRING),
            ("Type", DataType.STRING),
            ("Phase", DataType.STRING),
            ("DestinationPhasorID", DataType.INT32),
            ("SourceIndex", DataType.INT32),
            ("UpdatedOn", DataType.DATETIME),
        ])
        dataset.add_table(table)

        for phasor_id, p in enumerate(self.phasors, start=1):
            row = table.create_row()
            row["ID"] = np.int32(phasor_id)
            row["DeviceAcronym"] = self.device.acronym
            row["Label"] = p.label
            row["Type"] = p.phasor_type
            row["Phase"] = p.phase
            row["DestinationPhasorID"] = np.int32(0)
            row["SourceIndex"] = np.int32(p.source_index)
            row["UpdatedOn"] = now
            table.add_row(row)


# --- small helpers -----------------------------------------------------------

def _add_columns(table, columns) -> None:
    for name, datatype in columns:
        table.add_column(table.create_column(name, datatype))


def _sttp_timestamp() -> str:
    # Same "yyyy-MM-ddTHH:mm:ss.fff±HH:MM" shape the XML path writes.
    now = datetime.now(timezone.utc)
    offset = datetime.now().astimezone().strftime("%z")
    return now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + f"{offset[:3]}:{offset[3:]}"
