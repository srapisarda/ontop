#!/bin/sh

# Variable loading
source variables.sh

# -------
# SESAME
# -------

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

cd $ONTOP_DEP_HOME
wget http://eclipse.mirror.garr.it/mirrors/eclipse//jetty/stable-8/dist/${JETTY_DOWNLOADED_FILE}
mv ${JETTY_DOWNLOADED_FILE} ${JETTY_FILE}

#---------
# Protege
#---------

cd $ONTOP_DEP_HOME
wget http://protege.stanford.edu/download/protege/${PROTEGE_MAIN_VERSION}/zip/${PROTEGE_DL_FILE}
mv ${PROTEGE_DL_FILE} ${PROTEGE_FILE}

#------------------
# Protege plugins
#------------------

cd $ONTOP_DEP_HOME
wget https://github.com/ontop/ontop-dependencies/raw/master/org.protege.osgi.jdbc.jar
wget https://github.com/ontop/ontop-dependencies/raw/master/org.protege.osgi.jdbc.prefs.jar

