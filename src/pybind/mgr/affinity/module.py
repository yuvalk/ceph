
"""
A Affinity world module

See doc/mgr/affinity.rst for more info.
"""

from mgr_module import MgrModule, CommandResult, HandleCommandResult
from threading import Event

import os
import json
import subprocess
from tempfile import mkstemp
from mako.template import Template

class Affinity(MgrModule):
    # these are CLI commands we implement
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

    # these are module options we understand.  These can be set with
    # 'ceph config set global mgr/hello/<name> <value>'.  e.g.,
    # 'ceph config set global mgr/hello/place Earth'
    MODULE_OPTIONS = [
    ]

    def __init__(self, *args, **kwargs):
        super(Affinity, self).__init__(*args, **kwargs)

        #self.osdmap = self.get('osd_map')
        #self.osdmap_dump = self.osdmap.dump()
        #self.crush = self.osdmap.get_crush()
        #self.crush_dump = self.crush.dump()

        # set up some members to enable the serve() method and shutdown
        self.run = True
        self.event = Event()

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

        result = CommandResult('')
        self.send_command(result, 'mon', '', json.dumps({
            'prefix': 'osd getcrushmap'
        }), '')
        r, outb, outs = result.wait()
        if r != 0:
            self.log.error('Error dumping crush map')
            return

        with open("/tmp/af.log","w") as f:
            f.write(outb.__class__.__name__)

        with open(crushmap_filename,"wb") as f:
            f.write(bytearray(outb))

        #os.close(tempfd)
        return

        #crushmap_filename])
        subprocess.check_call(["crushtool","-d",crushmap_filename,"-o",editable_crushmap_filename])
        with open(editable_crushmap_filename,"a") as cmfile:
            cmfile.write(rule_template.render(
                name=name, 
                rule_id=get_highest_rule_id()+1,
                region=region,
                primary_domain=pdomain,
                secondary_domain=sdomain,
                tertiary_domain=tdomain
            ))
        subprocess.check_call(["crushtool","-c",editable_crushmap_filename,"-o",crushmap_filename])
        subprocess.call(["ceph","osd","setcrushmap","-i",crushmap_filename])

#        os.remove(crushmap_filename)
#        os.remove(editable_crushmap_filename)

    def handle_command(self, inbuf, cmd):
        self.log.debug("Handling command: '%s'" % str(cmd))
        self.log.error("uid is: " + str(os.getuid()))
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
        output_buffer = "Output buffer is for data results"
        output_string = "Output string is for informative text"

        message=cmd['prefix']
        return HandleCommandResult(retval=status_code, stdout=output_buffer,
                                   stderr=message + "\n" + output_string)

    def serve(self):
        """
        This method is called by the mgr when the module starts and can be
        used for any background activity.
        """
        self.log.info("Starting")
        while self.run:
            sleep_interval = 5
            self.log.debug('Sleeping for %d seconds', sleep_interval)
            ret = self.event.wait(sleep_interval)
            self.event.clear()

    def shutdown(self):
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit.
        """
        self.log.info('Stopping')
        self.run = False
        self.event.set()
