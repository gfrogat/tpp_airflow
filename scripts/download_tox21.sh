#!/bin/env bash

TOX21_DIR="/publicdata/tpp/datasets/Tox21"
TOX21_ALL_URL="https://tripod.nih.gov/tox21/challenge/download?id=tox21_10k_data_allsdf"
TOX21_TEST_URL="https://tripod.nih.gov/tox21/challenge/download?id=tox21_10k_challenge_testsdf"
TOX21_SCORE_URL="https://tripod.nih.gov/tox21/challenge/download?id=tox21_10k_challenge_scoresdf"

wget -P ${TOX21_DIR} -O tox21_10k_data_all.sdf.zip "${TOX21_ALL_URL}"
wget -P ${TOX21_DIR} -O tox21_10k_data_test.sdf.zip "${TOX21_TEST_URL}"
wget -P ${TOX21_DIR} -O tox21_10k_data_score.sdf.zip "${TOX21_SCORE_URL}"
