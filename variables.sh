#!/bin/sh

# location for the build ROOT folder
export BUILD_ROOT=/build/ontop

# location for the build dependencies home
export ONTOP_DEP_HOME=/build/dependencies


export SESAME_VERSION=2.7.13
export SESAME_PREFIX=openrdf-sesame-${SESAME_VERSION}
export SESAME_SDK_FILE_PREFIX=${SESAME_PREFIX}-sdk
export SESAME_SDK_FILE=${SESAME_SDK_FILE_PREFIX}.tar.gz
export SESAME_WAR_FILE=openrdf-sesame.war
export SESAME_WORKBENCH_WAR_FILE=openrdf-workbench.war
#location for sesame and workbench WEB-APP jars
export OPENRDF_WORKBENCH_PATH=${ONTOP_DEP_HOME}
export OPENRDF_SESAME_PATH=${ONTOP_DEP_HOME}
# name of the wars for sesame and workbench WEB-APPs  (these have to be already customized with stylesheets)
export OPENRDF_SESAME_FILENAME=openrdf-sesame
export OPENRDF_WORKBENCH_FILENAME=openrdf-workbench

export JETTY_VERSION_NB=8.1.16
export JETTY_VERSION_DAY=v20140903
export JETTY_VERSIONED_PREFIX=jetty-distribution-${JETTY_VERSION_NB}.${JETTY_VERSION_DAY}
export JETTY_DOWNLOADED_FILE=${JETTY_VERSIONED_PREFIX}.zip
export JETTY_FILE=jetty-distribution.zip
export JETTY_COPY_PATH=${ONTOP_DEP_HOME}
export JETTY_INNER_FOLDERNAME=${JETTY_VERSIONED_PREFIX}

export PROTEGE_MAIN_VERSION=4.3
export PROTEGE_MINOR_REVISION=0
export PROTEGE_VERSION_SUFFIX=304
export PROTEGE_DL_FILE=protege-${PROTEGE_MAIN_VERSION}.${PROTEGE_MINOR_REVISION}-${PROTEGE_VERSION_SUFFIX}.zip
export PROTEGE_FILE=protege.zip
export PROTEGE_COPY_PATH=${ONTOP_DEP_HOME}
export PROTEGE_COPY_FILENAME=protege
export PROTEGE_MAIN_FOLDER_NAME=Protege_${PROTEGE_MAIN_VERSION}
export PROTEGE_MAIN_PLUGIN=ontopro-plugin

# location for the JDBC plugin jars
export JDBC_PLUGINS_PATH=${ONTOP_DEP_HOME}

# folder names of the output
export ONTOP_DIST_DIR=${BUILD_ROOT}/quest-distribution
export PROTEGE_DIR=${ONTOP_DIST_DIR}/ontopPro
export QUEST_SESAME_DIR=${ONTOP_DIST_DIR}/QuestSesame
export QUEST_JETTY_DIR=${ONTOP_DIST_DIR}/QuestJetty
export OWL_API_DIR=${ONTOP_DIST_DIR}/QuestOWL

export VERSION=1.13
export REVISION=2-SNAPSHOT


