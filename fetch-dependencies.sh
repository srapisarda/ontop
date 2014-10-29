#!/bin/sh

export ONTOP_DEP_HOME=/build/dependencies

# -------
# SESAME
# -------

export SESAME_VERSION=2.7.13
export SESAME_PREFIX=openrdf-sesame-${SESAME_VERSION}
export SESAME_SDK_FILE_PREFIX=${SESAME_PREFIX}-sdk
export SESAME_SDK_FILE=${SESAME_SDK_FILE_PREFIX}.tar.gz
export SESAME_WAR_FILE=openrdf-sesame.war
export SESAME_WORKBENCH_WAR_FILE=openrdf-workbench.war

cd $ONTOP_DEP_HOME
wget http://downloads.sourceforge.net/project/sesame/Sesame%202/${SESAME_VERSION}/${SESAME_SDK_FILE}
tar -xzf $SESAME_SDK_FILE
rm ${SESAME_SDK_FILE}
mv $SESAME_PREFIX/war/${SESAME_WAR_FILE} .
mv $SESAME_PREFIX/war/${SESAME_WORKBENCH_WAR_FILE} .
rm -r ${SESAME_PREFIX}


#--------
# JETTY
#--------

export JETTY_VERSION_NB=8.1.16
export JETTY_VERSION_DAY=v20140903
export JETTY_DOWNLOADED_FILE=jetty-distribution-${JETTY_VERSION_NB}.${JETTY_VERSION_DAY}.zip
export JETTY_FILE=jetty-distribution.zip

cd $ONTOP_DEP_HOME
wget http://eclipse.mirror.garr.it/mirrors/eclipse//jetty/stable-8/dist/${JETTY_DOWNLOADED_FILE}
mv ${JETTY_DOWNLOADED_FILE} ${JETTY_FILE}

#---------
# Protege
#---------

cd $ONTOP_DEP_HOME
export PROTEGE_MAIN_VERSION=4.3
export PROTEGE_MINOR_REVISION=0
export PROTEGE_VERSION_SUFFIX=304
export PROTEGE_DL_FILE=protege-${PROTEGE_MAIN_VERSION}.${PROTEGE_MINOR_REVISION}-${PROTEGE_VERSION_SUFFIX}.zip
export PROTEGE_FILE=protege.zip
wget http://protege.stanford.edu/download/protege/${PROTEGE_MAIN_VERSION}/zip/${PROTEGE_DL_FILE}
mv ${PROTEGE_DL_FILE} ${PROTEGE_FILE}

#------------------
# Protege plugins
#------------------

cd $ONTOP_DEP_HOME
wget https://github.com/ontop/ontop-dependencies/raw/master/org.protege.osgi.jdbc.jar
wget https://github.com/ontop/ontop-dependencies/raw/master/org.protege.osgi.jdbc.prefs.jar

