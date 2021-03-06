import os
import subprocess
from hdpstat import table_utils

def generate_pls (items, title, yaxis, mult=1, suffix="", area=True, color_idx=None, data=""):
    """
    Produce ploticus script for area plot. Return filename of produced pls
    """
    if len (suffix) > 0:
        yaxis += ", " + suffix

    data = """
#proc getdata
  delim: comma
  data:
%(data)s


//#proc page
//  pixsize: 640 480

#proc areadef
  title: %(title)s
  titledetails: align=C size=10 adjust=0,0.2
  xscaletype: datetime yyyy-mm-dd hh:mm
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
      label: %(yaxis)s
      labeldetails: adjust=-0.2,0
      grid: color=gray(0.8) style=1
      stubformat: autoround
      stubmult: %(mult)s
""" % {'data': data, 'title': title, 'item_indices': ",".join (map (str, range (2, len (items)+2))),
       'yaxis': yaxis, 'mult': mult}
    colors = ['oceanblue', 'yellowgreen', 'orange', 'tan2', 'green', 'darkblue',
			              'purple', 'red', 'coral', 'gray(0.3)', 'teal', 'magenta', 'skyblue',
						                'lightorange', 'tan1', 'lavender', 'pink', 'claret', 'lightpurple', 
										              'redorange', 'black']*5

    for i, item in enumerate (reversed (items)):
        if color_idx == None:
            color = colors[i]
        else:
            color = colors[color_idx]
        data += """
#proc lineplot
  xfield: 1
  yfield: %(index)d
  legendlabel: %(label)s
""" % {'index': 1+len (items) - i, 'label': item}
        if area:
            data += "fill: %s\n" % color
        else:
            data += "linedetails: color=%s\n" % color

        
    if len (items) > 1:
        data += """
#proc legend
      location: min+0.5 max+0.4
"""
    fname = os.tmpnam ()
    with open (fname, 'w+') as fd:
        fd.write (data)
    return fname



def format_chart_data (keys, data_table, aggregate=True):
    """
    Format data to string ready to be fed to ploticus
    """
    dates = data_table.keys ()
    dates.sort ()

    # prepare data string
    chart_data = ""
    for date in dates:
        chart_data += date.strftime ("%Y-%m-%d.%H:%M")
        s = 0
        for k in keys:
            val = data_table[date].get (k, 0)
            if aggregate:
                s += val
            else:
                s = val
            chart_data += ",%s" % s
        chart_data += "\n"
    return chart_data


def generate_chart (pls_file):
    # start ploticus
    imgname = os.tmpnam ()

    p = subprocess.Popen (["pl", "-font", "FreeSans", "-png", "-o", imgname, pls_file],
                          env={"GDFONTPATH": os.getcwd ()},
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = p.communicate ()
    if p.returncode == 0:
        with open (imgname, 'rb') as fd:
            data = fd.read ()
        os.unlink (imgname)
        return data
    else:
        return None


def data_multiplier (data_table, binary=True):
    # obtain max value from data_table
    max_val = 0
    for arr in data_table.values ():
        max_val = max (arr.values () + [max_val])

    return table_utils.LargeNumberColumn.best_suffix_and_mul (max_val, binary=binary)


