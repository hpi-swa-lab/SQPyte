import csv, sys
import matplotlib.pyplot as plt
import numpy as np

def get_data(filename):
    with open(filename, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        ret = []
        for row in reader:
            ret.append(row)
    return ret

def process_data(data):
    retMean = []
    retStd = []
    for i in range(0, len(data)):
        data[i].pop(0)
        for j in range(0, len(data[i])):
            data[i][j] = float(data[i][j])
        retMean.append(np.mean(data[i]))
        retStd.append(np.std(data[i]))
    return retMean, retStd

def geomean(num_list):
    return sum(num_list) ** (1.0/len(num_list))

def normalise_data(base, data):
    ret = []
    for i in range(len(base)):
        ret.append(data[i]/base[i])
    geom = geomean(ret)
    return ret, geom    

def get_ticklabels(n):
    ret = []
    for i in range(1, n + 1):
        ret.append('Q%s' % i)
    return ret

def create_absolute_plot(series1, std1, series2, std2, outfile):
    N = len(series1)
    ind = np.arange(N)  # the x locations for the groups
    width = 0.35        # the width of the bars

    # define plot size in inches (width, height) & resolution(DPI)
    fig, ax = plt.subplots(figsize=(12, 5), dpi=100)

    # define font size
    plt.rc("font", size=9)

    rects1 = ax.bar(ind, series1, width, color='r', yerr=std1)
    rects2 = ax.bar(ind + width, series2, width, color='g', yerr=std2)

    ax.set_ylabel('Runtime (seconds)', size=10)
    ax.set_title('SQLite3 Performance vs. SQPyte Performance')
    ax.set_xticks(ind + width)

    # offset for bars
    plt.xlim(-0.5, len(series1) + 0.25)
    plt.ylim(0, max(series1 + series2) + 10)

    # do not display top and right axes
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)

    # outward ticks
    ax.tick_params(direction='out')

    # remove unneeded ticks 
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()

    # horizontal grid lines
    ax.yaxis.grid(True)
    ax.set_axisbelow(True)

    ax.set_xticklabels(get_ticklabels(len(series1)))

    ax.legend((rects1[0], rects2[0]), ('SQLite3', 'SQPyte'), loc=2)

    # def autolabel(rects):
    #     for rect in rects:
    #         height = rect.get_height()
    #         ax.text(rect.get_x() + rect.get_width() / 2., 1.05 * height, '%d' % int(height),
    #                 ha='center', va='bottom')
    # autolabel(rects1)
    # autolabel(rects2)

    # plt.show()
    fig.savefig(outfile, bbox_inches="tight")
    plt.clf()

def create_normalised_plot(series, outfile):
    N = len(series)
    ind = np.arange(N)  # the x locations for the groups
    width = 0.35        # the width of the bars

    # define plot size in inches (width, height) & resolution(DPI)
    fig, ax = plt.subplots(figsize=(13, 5), dpi=100)

    # define font size
    plt.rc("font", size=9)

    rects1 = ax.bar(ind, series, width, color='g')

    # colouring bars
    for i in range(len(series)):
        if series[i] > 1:
            rects1[i].set_color('r')

    # GM bar is of different colour
    rects1[len(series) - 1].set_color('b')

    ax.set_ylabel('Normalised Runtime', size=10)
    ax.set_title('SQLite3 Performance vs. SQPyte Performance')
    ax.set_xticks(ind + width - 0.175)

    # offset for bars
    plt.xlim(-0.5, len(series) + 0.1)
    plt.ylim(0, max(series) + 0.4)

    # do not display top and right axes
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)

    # remove unneeded ticks 
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()

    # outward ticks
    ax.tick_params(direction='out')

    # horizontal line
    ax.axhline(y=1, xmin=0, xmax=100, c='k', linewidth=0.5, linestyle='dotted', zorder=0)

    ax.set_xticklabels(get_ticklabels(len(series) - 1) + ['GM'])

    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2., 1.05 * height, '%.2fx' % float(height),
                    ha='center', va='bottom', size=7)
    autolabel(rects1)

    # plt.show()
    fig.savefig(outfile, bbox_inches="tight")
    plt.clf()


def run(argv):
    usageMsg = """\n
  Usage:
  python script.py data-set-1.csv data-set-2.csv [output-file-name.pdf]
    """

    if len(argv) < 2:
        print "Not enough arguments. %s" % usageMsg
        return 1
    else:
        if len(argv) > 3:
            outfile = argv[3]
        else:
            outfile = "out.pdf"

        data1 = get_data(argv[1])
        data2 = get_data(argv[2])
        series1, std1 = process_data(data1)
        series2, std2 = process_data(data2)

        # create_absolute_plot(series1, std1, series2, std2, outfile)
        
        normaldata, geom = normalise_data(series1, series2)
        series3 = normaldata + [geom]

        create_normalised_plot(series3, outfile)


if __name__ == "__main__":
    run(sys.argv)

