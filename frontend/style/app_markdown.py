# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import streamlit as st

MARKDOWN_STR_1 = """
<style>
button[kind="primary"] {
    background: none!important;
    border: 0;
    padding: 20!important;
    color: white !important;
    text-decoration: none;
    cursor: pointer;
    border: none !important;
    # float: right;
}
button[kind="primary"]:hover {
    text-decoration: none;
    color: white !important;
}
button[kind="primary"]:focus {
    outline: none !important;
    box-shadow: none !important;
    color:  !important;
}
</style>
"""
MARKDOWN_STR_2 = """
    <style>
    [data-testid="stSidebar"] {
        position: fixed;
        width: 375px;
    }

    [data-testid="stSidebarNav"] {
        font-size: 40px;
        font-weight: bold;
    }
    </style>
"""
def add_app_name_header():
    app_name = " Data Product Discovery and Design Assistant"

    st.markdown(f"""
    <style>
        .fixed-header-appname {{
            position: absolute;
            top: 0;
            left: 0;
            z-index: 999;
            padding: 10px;
            background-color: rgba(255, 255, 255, 0.0); 
            color: black;
            font-family: "VodafoneRegular";
            font-size: 30px;
            font-weight: bolder;
            border-bottom-left-radius: 5px;
            margin: 0px
        }}
    </style>
    <div class="fixed-header-appname">
        {app_name}
    </div>
    """,
    unsafe_allow_html = True
    )