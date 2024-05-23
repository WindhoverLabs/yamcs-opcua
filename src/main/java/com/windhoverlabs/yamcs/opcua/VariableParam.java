package com.windhoverlabs.yamcs.opcua;

import org.yamcs.xtce.DataSource;
import org.yamcs.xtce.NameDescription;
import org.yamcs.xtce.Parameter;
import org.yamcs.xtce.SystemParameter;
import org.yamcs.xtce.XtceDb;

public class VariableParam extends Parameter {
    private static final long serialVersionUID = 2L;

    private VariableParam(String spaceSystemName, String name, DataSource ds) {
        super(name);
        setQualifiedName(spaceSystemName + "/" + name);
        setDataSource(ds);
    }

    public static VariableParam getForFullyQualifiedName(String fqname) {
        DataSource ds = getSystemParameterDataSource(fqname);
        VariableParam sp = new VariableParam(NameDescription.getSubsystemName(fqname),
                NameDescription.getName(fqname), ds);
        // set the recording name "/instruments/tvac/b/c" -> "/instruments/tvac/"
        int pos = fqname.indexOf(PATH_SEPARATOR, 0);
        pos = fqname.indexOf(PATH_SEPARATOR, pos + 1);
        pos = fqname.indexOf(PATH_SEPARATOR, pos + 1);
        sp.setRecordingGroup(fqname.substring(0, pos));

        return sp;
    }

    private static DataSource getSystemParameterDataSource(String fqname) {
        if (fqname.startsWith(XtceDb.YAMCS_CMD_SPACESYSTEM_NAME)) {
            return DataSource.COMMAND;
        } else if (fqname.startsWith(XtceDb.YAMCS_CMDHIST_SPACESYSTEM_NAME)) {
            return DataSource.COMMAND_HISTORY;
        } else {
            return DataSource.SYSTEM;
        }
    }

    @Override
    public String toString() {
        return "SysParam(qname=" + getQualifiedName() + ")";
    }
}
