pjk "{id:1, cars:[{color: 'green', size:1}, {color: 'blue', size:10}]}" [ let:foo:bar reduce:^total+='f.size' over:cars \
    expect:"{id: 1, 'cars': [{color: 'green', size: 1, foo: 'bar'}, {color: 'blue', size: 10, foo: 'bar'}], total: 11}"

pjk '{id:2}' let:foo:'{bar}' -
									  
