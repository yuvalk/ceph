
"""
affinity module is aimed at managing multiple availability zone situations
that need local read affinity.
local read affinity in ceph means that all primary OSDs of a pool are from the same az.

See doc/mgr/affinity.rst for more info.

currently the only implemented feature is the ability to create such pool, by creating the correct CRUSH rule.
in the futuer we will implement more features to monitor and manage failures, etc.
"""

from mgr_module import MgrModule, CommandResult, HandleCommandResult
from threading import Event

import os
import json
import subprocess
from tempfile import mkstemp
from mako.template import Template


class Affinity(MgrModule):
    COMMANDS = [
        {
            "cmd": "affinity create pool "
                   "name=poolname,type=CephString,req=true "
                   "name=region,type=CephString,req=true "
                   "name=pd,type=CephString,req=true "
                   "name=sd,type=CephString,req=true "
                   "name=td,type=CephString,req=true",
            "desc": "create a new pool with primary affinity to pd",
            "perm": "rw"
        },
    ]

    def __init__(self, *args, **kwargs):
        super(Affinity, self).__init__(*args, **kwargs)

        # set up some members to enable the serve() method and shutdown
        self.run = True
        self.event = Event()

    def get_highest_rule_id(self):
        crush = self.get('osd_map_crush')
        crush_rules = crush['rules']
        id = 1
        for rule in crush_rules:
            if rule['rule_id'] > id:
                id = rule['rule_id']

        return id

    def create_crush_affined_rule(self, name, region, pd, sd, td):
        rule_template = Template(""" rule ${name} {
    id ${rule_id}
    type replicated
    min_size 1
    max_size 10
    step take ${region}${primary_domain}
    step chooseleaf firstn 1 type host
    step emit
    step take ${region}${secondary_domain}
    step chooseleaf firstn 1 type host
    step emit
    step take ${region}${tertiary_domain}
    step chooseleaf firstn 1 type host
    step emit
}""")
        
        tempfd, crushmap_filename = mkstemp(dir='/tmp')

        editable_crushmap_filename = crushmap_filename+".editable"

        #get binary crush map
        ret, buf, errs = self.rados.mon_command(json.dumps({"prefix":"osd getcrushmap"}), b'')
        if ret != 0:
            raise RuntimeError("Error getting binary crushmap. ret {}: '{}'".format(
                ret, err))

        # write binary crush map to file for crushtool
        with open(crushmap_filename,"wb") as f:
            f.write(bytearray(buf))

        os.close(tempfd)

        # decompile binary crush map
        subprocess.check_call(["crushtool","-d",crushmap_filename,"-o",editable_crushmap_filename])

        # write new rule to editable crush map, using template
        with open(editable_crushmap_filename,"a") as cmfile:
            cmfile.write(rule_template.render(
                name=name, 
                get_highest_rule_id()+1,
                region=region,
                primary_domain=pd,
                secondary_domain=sd,
                tertiary_domain=td
            ))

        # compile editable crush map
        subprocess.check_call(["crushtool","-c",editable_crushmap_filename,"-o",crushmap_filename])

        # read compiled, binary crush map
        with open(crushmap_filename,"rb") as f:
            barr = f.read()

        # apply binary crush map
        ret, buf, errs = self.rados.mon_command(json.dumps({"prefix":"osd setcrushmap"}), barr)
        if ret != 0:
            raise RuntimeError("Error setting binary crushmap. ret {}: '{}'".format(
                ret, err))

        # remove temporary files
        os.remove(crushmap_filename)
        os.remove(editable_crushmap_filename)

    def handle_command(self, inbuf, cmd):
        self.log.debug("Handling command: '%s'" % str(cmd))
        message=cmd['prefix']
        message += "uid: " 
        message += str(os.getuid())

        if cmd['prefix'] == "affinity create pool":
            self.create_crush_affined_rule(
                    cmd['poolname'] + "rule",
                    cmd['region'],
                    cmd['pd'],
                    cmd['sd'],
                    cmd['td'])
            self.log.debug("after create rule")
            
        status_code = 0
        output_buffer = ""
        output_string = ""

        message=cmd['prefix']
        return HandleCommandResult(retval=status_code, stdout=output_buffer,
                                   stderr=message + "\n" + output_string)

    def serve(self):
        self.log.info("Starting")
        while self.run:
            sleep_interval = 5
            self.log.debug('Sleeping for %d seconds', sleep_interval)
            ret = self.event.wait(sleep_interval)
            self.event.clear()

    def shutdown(self):
        self.log.info('Stopping')
        self.run = False
        self.event.set()
