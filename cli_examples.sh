#!/bin/zsh
# create search service in Basic SKU (stock keeping unit)
az search service create -n mdsearch4321 -g DataProfessionalPathway2024 -l uksouth --sku Standard
# show admin keys
az search admin-key show -g DataProfessionalPathway2024 --service-name mdsearch4321
