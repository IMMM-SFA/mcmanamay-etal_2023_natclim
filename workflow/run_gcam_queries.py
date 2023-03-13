import os
import argparse
import pandas as pd
import gcamreader

# embed queries in the script

ag_output_query = gcamreader.Query("""<aQuery>
<supplyDemandQuery title="ag production by tech">
<axis1 name="technology">technology[@name]</axis1>
<axis2 name="Year">physical-output[@vintage]</axis2>
<xPath buildList="true" dataName="output" group="false" sumAll="false">*[@type='sector' and (local-name()='AgSupplySector')]/*[@type='subsector']/*[@type='technology']//physical-output/node()</xPath>
<comments/>
</supplyDemandQuery>
</aQuery>""")

detailed_land_query = gcamreader.Query("""<aQuery>
<query title="detailed land allocation">
<axis1 name="LandLeaf">LandLeaf[@name]</axis1>
<axis2 name="Year">land-allocation[@year]</axis2>
<xPath buildList="true" dataName="LandLeaf" group="false" sumAll="false">/LandNode[@name='root' or @type='LandNode' (:collapse:)]//land-allocation/text()</xPath>
<comments/>
</query>
</aQuery>""")

co2_emissions_query = gcamreader.Query("""<aQuery>
<emissionsQueryBuilder title="CO2 emissions by sector">
<axis1 name="sector">sector</axis1>
<axis2 name="Year">emissions</axis2>
<xPath buildList="true" dataName="emissions" group="false" sumAll="false">*[@type='sector']//CO2/emissions/node()</xPath>
<comments/>
</emissionsQueryBuilder>
</aQuery>""")

luc_emissions_query = gcamreader.Query("""<aQuery>
<query title="LUC emissions by LUT">
<axis1 name="LandLeaf">LandLeaf</axis1>
<axis2 name="Year">land-use-change-emission[@year]</axis2>
<xPath buildList="true" dataName="land-use-change-emission" group="false" sumAll="false">/LandNode[@name='root' or @type='LandNode' (:collapse:)]//land-use-change-emission[@year&gt;1970]/text()</xPath>
<comments/>
</query>
</aQuery>""")


def main(inputdir, outputdir):

    ag_output_all = pd.DataFrame()
    co2_emissions_all = pd.DataFrame()
    luc_emissions_all = pd.DataFrame()

    for dbfile in os.listdir(inputdir):

        if os.path.isdir(os.path.join(inputdir, dbfile)):

            conn = gcamreader.LocalDBConn(inputdir, dbfile)

            # agricultural output
            ag_output = conn.runQuery(ag_output_query)
            # filter to Megatonnes
            ag_output = ag_output[ag_output['Units'] == 'Mt']
            ag_output = ag_output.drop(columns=['Units']).rename(columns=dict(value='agricultural output (Mt)'))

            # detailed land allocation
            land = conn.runQuery(detailed_land_query)
            land = land.drop(columns=['Units', 'scenario']).rename(columns=dict(LandLeaf='technology', value='area (thous km2)'))

            # join land area to ag output to compute yield
            ag_output = ag_output.join(land.set_index(['region', 'technology', 'Year']), on=['region', 'technology', 'Year'])
            ag_output['yield (Mt per thous km2)'] = ag_output['agricultural output (Mt)'] / ag_output['area (thous km2)']
            ag_output_all = pd.concat([ag_output_all, ag_output])

            # CO2 emissions
            co2_emissions = conn.runQuery(co2_emissions_query)
            co2_emissions_all = pd.concat([co2_emissions_all, co2_emissions])

            # LUC emissions
            luc_emissions = conn.runQuery(luc_emissions_query)
            luc_emissions_all = pd.concat([luc_emissions_all, luc_emissions])

    # write outputs
    ag_output_all.to_csv(os.path.join(outputdir, 'ag_output.csv.gz'), index=False)
    co2_emissions_all.to_csv(os.path.join(outputdir, 'co2_emissions.csv.gz'), index=False)
    luc_emissions_all.to_csv(os.path.join(outputdir, 'luc_emissions.csv.gz'), index=False)


if __name__ == '__main__':

    # get input and output directories
    parser = argparse.ArgumentParser()
    parser.add_argument('inputdir')
    parser.add_argument('outputdir')
    args = parser.parse_args()

    # run queries, write outputs
    main(args.inputdir, args.outputdir)
