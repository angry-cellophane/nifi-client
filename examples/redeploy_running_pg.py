from nifi import Nifi

"""
This is an exaple showing how to update a running process group from a template
1. Find the parent group
2. Create a new group from a given template
3. Stop the existent group
4. Empty the existent group's queues
5. Start the new group
6. Remove the old group
7. Rename the new group and adjust its position
"""
server = Nifi('http://localhost:8080')

flow_res = server.resource('flow')
pg_res = server.resource('process-groups')
queues_res = server.resource('flowfile-queues')

parent_pg = next(pg for pg in flow_res.list_pgs() if pg['name'] == 'Example: update running group')
print('Found parent group = {"id": "%s", "name":"%s"}' % (parent_pg['id'], parent_pg['name']))

child_pgs = pg_res.list_children(parent_pg['id'], 'process-groups')
old_pg = next(pg for pg in child_pgs if pg['component']['name'] == 'Running Group')
old_position = old_pg['component']['position']
print('Found the group to be replaced: {"id": "%s", "name": "%s"}' %
      (old_pg['id'], old_pg['component']['name']))

template = next(t for t in flow_res.list_templates() if t['template']['name']
                == 'Example: redeploying running group')
print('Found the template to be used: {"id": "%s", "name": "%s"}' %
      (template['id'], template['template']['name']))

template_req = {'templateId': template['id'], 'originX': old_position['x'] +
                400, 'originY': old_position['y']}
tmpl_inst = pg_res.instantiate_template(parent_pg['id'], template_req)
assert len(tmpl_inst['processGroups']) == 1
new_pg = tmpl_inst['processGroups'][0]
print('New process group created: {"id": "%s", "name": "%s"}' % (new_pg['id'], new_pg['component']['name']))

flow_res.stop_pg(old_pg['id'])

print("Dropping requests from the old process group's queues")
for connection in pg_res.list_children(old_pg['id'], 'connections'):
    queues_res.drop_requests(connection['id'])
    print('Dropped requests from %s' % (connection['id']))

flow_res.start_pg(new_pg['id'])
pg_res.delete(old_pg)
print('Old group %s removed' % (old_pg['id']))

new_pg['component']['name'] = old_pg['component']['name']
new_pg['component']['position'] = old_pg['component']['position']
pg_res.update(new_pg)
print('New group renamed to %s' % (new_pg['component']['name']))
print('The group %s successfully updated from the %s template' %
      (new_pg['component']['name'], template['template']['name']))
