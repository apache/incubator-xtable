# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements…
# (your license header…)

PYTHON  := python3
VENV_DIR := .venv
PIP     := $(VENV_DIR)/bin/pip
VALIDATOR := $(VENV_DIR)/bin/openapi-spec-validator
SPEC    := rest-service-open-api.yaml

# Configuration for model generation
GEN_DIR     := generated
MODEL_PKG   := org.apache.xtable.service.models

.PHONY: all venv install lint clean clean-models

all: lint

# Create venv if it doesn't exist
venv:
	@echo "→ creating virtualenv in $(VENV_DIR)…"
	$(PYTHON) -m venv $(VENV_DIR)
	@echo "→ upgrading pip…"
	$(PIP) install --upgrade pip

# Install requirements into venv
install: venv
	@echo "→ installing validator…"
	$(PIP) install -r requirements.txt

# Validate your OpenAPI spec
lint: install
	@echo "→ linting $(SPEC)…"
	$(VALIDATOR) --errors all $(SPEC)

# Remove the virtualenv
clean:
	@echo "→ removing $(VENV_DIR)…"
	rm -rf $(VENV_DIR)

# Generate Java model classes from OpenAPI spec
generate-models:
	@echo "→ generating Java model classes from $(SPEC)…"
	openapi-generator generate \
	  -i $(SPEC) \
	  -g java \
	  -o $(GEN_DIR) \
	  --model-package $(MODEL_PKG) \
	  --global-property models,modelTests=false,modelDocs=false
	@echo "→ models generated in $(GEN_DIR)/src/main/java/$(subst .,/,$(MODEL_PKG))"

# Remove all generated model classes
clean-models:
	@echo "→ removing generated models in $(GEN_DIR)…"
	rm -rf $(GEN_DIR)