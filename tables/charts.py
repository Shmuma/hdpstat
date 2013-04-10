import os

def generate_area_pls (items, title):
    """
    Produce ploticus script for area plot. Return filename of produced pls
    """
    data = """
#proc getdata
  standardinput: yes
  delim: comma
//  showdata: yes

#proc areadef
  title: %(title)s
  titledetails: align=C size=13 adjust=0,0.2
  xscaletype: datetime yyyy-mm-dd hh:mm:ss
  yscaletype: linear
  xautorange: datafield=1
  yautorange: datafield=%(item_indices)s

#proc xaxis
      axisline: width=0.7 color=black
      grid: color=gray(0.8) style=1
      stubs: inc

#proc yaxis
      axisline: width=0.7 color=black
      stubs: inc
      label: Size, TB
      labeldetails: adjust=-0.2,0
      grid: color=gray(0.8) style=1
      stubformat: autoround
      stubmult: 0.000000000001
""" % {'title': title, 'item_indices': ",".join (map (str, range (2, len (items)+2)))}

    colors = ['red', 'yellowgreen', 'orange', 'yellow', 'green', 'darkblue',
              'purple', 'oceanblue', 'coral', 'gray(0.3)', 'teal', 'magenta']

    for i, item in enumerate (reversed (items)):
        data += """
#proc lineplot
  fill: %(color)s
  yfield: %(index)d
  legendlabel: %(label)s
""" % {'index': 1+len (items) - i, 'label': item, 'color': colors[i]}
        

    data += """
#proc legend
      location: min+0.5 max+0.4
"""
    fname = os.tmpnam ()
    with open (fname, 'w+') as fd:
        fd.write (data)
    return fname
