# coding: utf-8
from sqlalchemy import BigInteger, Column, Date, ForeignKey,\
    Numeric, Text, text
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()
metadata = Base.metadata


class Ad(Base):
    __tablename__ = 'ad'
    __table_args__ = {'schema': 'lqadb'}

    adid = Column(BigInteger, primary_key=True,
                  server_default=text("nextval('lqadb.ad_adid_seq'::regclass)"))
    adname = Column(Text)


class Adformat(Base):
    __tablename__ = 'adformat'
    __table_args__ = {'schema': 'lqadb'}

    adformatid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.adformat_adformatid_seq'::regclass)"))
    adformatname = Column(Text)


class Adsize(Base):
    __tablename__ = 'adsize'
    __table_args__ = {'schema': 'lqadb'}

    adsizeid = Column(
        BigInteger, primary_key=True, server_default=text(
            "nextval('lqadb.adsize_adsizeid_seq'::regclass)"))
    adsizename = Column(Text)


class Adtype(Base):
    __tablename__ = 'adtype'
    __table_args__ = {'schema': 'lqadb'}

    adtypeid = Column(
        BigInteger, primary_key=True,
        server_default=text("nextval('lqadb.adtype_adtypeid_seq'::regclass)"))
    adtypename = Column(Text)


class Agency(Base):
    __tablename__ = 'agency'
    __table_args__ = {'schema': 'lqadb'}

    agencyid = Column(
        BigInteger, primary_key=True,
        server_default=text("nextval('lqadb.agency_agencyid_seq'::regclass)"))
    agencyname = Column(Text)


class Buymodel(Base):
    __tablename__ = 'buymodel'
    __table_args__ = {'schema': 'lqadb'}

    buymodelid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.buymodel_buymodelid_seq'::regclass)"))
    buymodelname = Column(Text)


class Character(Base):
    __tablename__ = 'character'
    __table_args__ = {'schema': 'lqadb'}

    characterid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.character_characterid_seq'::regclass)"))
    charactername = Column(Text)


class Creativedescription(Base):
    __tablename__ = 'creativedescription'
    __table_args__ = {'schema': 'lqadb'}

    creativedescriptionid = Column(
        BigInteger, primary_key=True, server_default=text(
            "nextval('lqadb.creativedescription_creativedescriptionid_seq'::regclass)"))
    creativedescriptionname = Column(Text)


class Creativelength(Base):
    __tablename__ = 'creativelength'
    __table_args__ = {'schema': 'lqadb'}

    creativelengthid = Column(
        BigInteger, primary_key=True, server_default=text(
            "nextval('lqadb.creativelength_creativelengthid_seq'::regclass)"))
    creativelengthname = Column(Text)


class Creativelineitem(Base):
    __tablename__ = 'creativelineitem'
    __table_args__ = {'schema': 'lqadb'}

    creativelineitemid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.creativelineitem_creativelineitemid_seq'::regclass)"))
    creativelineitemname = Column(Text)


class Creativemodifier(Base):
    __tablename__ = 'creativemodifier'
    __table_args__ = {'schema': 'lqadb'}

    creativemodifierid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.creativemodifier_creativemodifierid_seq'::regclass)"))
    creativemodifiername = Column(Text)


class Cta(Base):
    __tablename__ = 'cta'
    __table_args__ = {'schema': 'lqadb'}

    ctaid = Column(
        BigInteger, primary_key=True,
        server_default=text("nextval('lqadb.cta_ctaid_seq'::regclass)"))
    ctaname = Column(Text)


class Datatype1(Base):
    __tablename__ = 'datatype1'
    __table_args__ = {'schema': 'lqadb'}

    datatype1id = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.datatype1_datatype1id_seq'::regclass)"))
    datatype1name = Column(Text)


class Datatype2(Base):
    __tablename__ = 'datatype2'
    __table_args__ = {'schema': 'lqadb'}

    datatype2id = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.datatype2_datatype2id_seq'::regclass)"))
    datatype2name = Column(Text)


class Demographic(Base):
    __tablename__ = 'demographic'
    __table_args__ = {'schema': 'lqadb'}

    demographicid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.demographic_demographicid_seq'::regclass)"))
    demographicname = Column(Text)


class Descriptionline1(Base):
    __tablename__ = 'descriptionline1'
    __table_args__ = {'schema': 'lqadb'}

    descriptionline1id = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.descriptionline1_descriptionline1id_seq'::regclass)"))
    descriptionline1name = Column(Text)


class Descriptionline2(Base):
    __tablename__ = 'descriptionline2'
    __table_args__ = {'schema': 'lqadb'}

    descriptionline2id = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.descriptionline2_descriptionline2id_seq'::regclass)"))
    descriptionline2name = Column(Text)


class Displayurl(Base):
    __tablename__ = 'displayurl'
    __table_args__ = {'schema': 'lqadb'}

    displayurlid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.displayurl_displayurlid_seq'::regclass)"))
    displayurlname = Column(Text)


class Environment(Base):
    __tablename__ = 'environment'
    __table_args__ = {'schema': 'lqadb'}

    environmentid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.environment_environmentid_seq'::regclass)"))
    environmentname = Column(Text)


class Faction(Base):
    __tablename__ = 'faction'
    __table_args__ = {'schema': 'lqadb'}

    factionid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.faction_factionid_seq'::regclass)"))
    factionname = Column(Text)


class Gender(Base):
    __tablename__ = 'gender'
    __table_args__ = {'schema': 'lqadb'}

    genderid = Column(
        BigInteger, primary_key=True,
        server_default=text("nextval('lqadb.gender_genderid_seq'::regclass)"))
    gendername = Column(Text)


class Genretargeting(Base):
    __tablename__ = 'genretargeting'
    __table_args__ = {'schema': 'lqadb'}

    genretargetingid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.genretargeting_genretargetingid_seq'::regclass)"))
    genretargetingname = Column(Text)


class Genretargetingfine(Base):
    __tablename__ = 'genretargetingfine'
    __table_args__ = {'schema': 'lqadb'}

    genretargetingfineid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.genretargetingfine_genretargetingfineid_seq'::regclass)"))
    genretargetingfinename = Column(Text)


class Headline1(Base):
    __tablename__ = 'headline1'
    __table_args__ = {'schema': 'lqadb'}

    headline1id = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.headline1_headline1id_seq'::regclass)"))
    headline1name = Column(Text)


class Headline2(Base):
    __tablename__ = 'headline2'
    __table_args__ = {'schema': 'lqadb'}

    headline2id = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.headline2_headline2id_seq'::regclass)"))
    headline2name = Column(Text)


class Kpi(Base):
    __tablename__ = 'kpi'
    __table_args__ = {'schema': 'lqadb'}

    kpiid = Column(
        BigInteger, primary_key=True,
        server_default=text("nextval('lqadb.kpi_kpiid_seq'::regclass)"))
    kpiname = Column(Text)


class Mediachannel(Base):
    __tablename__ = 'mediachannel'
    __table_args__ = {'schema': 'lqadb'}

    mediachannelid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.mediachannel_mediachannelid_seq'::regclass)"))
    mediachannelname = Column(Text)


class Model(Base):
    __tablename__ = 'model'
    __table_args__ = {'schema': 'lqadb'}

    modelid = Column(
        BigInteger, primary_key=True,
        server_default=text("nextval('lqadb.models_modelid_seq'::regclass)"))
    modeltype = Column(Text)
    modelname = Column(Text)
    modelcoefa = Column(Numeric)
    modelcoefb = Column(Numeric)
    modelcoefc = Column(Numeric)
    modelcoefd = Column(Numeric)
    eventdate = Column(Date)


class Packagedescription(Base):
    __tablename__ = 'packagedescription'
    __table_args__ = {'schema': 'lqadb'}

    packagedescriptionid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.packagedescription_packagedescriptionid_seq'::regclass)"))
    packagedescriptionname = Column(Text)


class Placement(Base):
    __tablename__ = 'placement'
    __table_args__ = {'schema': 'lqadb'}

    placementid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.placement_placementid_seq'::regclass)"))
    placementname = Column(Text)


class Placementdescription(Base):
    __tablename__ = 'placementdescription'
    __table_args__ = {'schema': 'lqadb'}

    placementdescriptionid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.placementdescription_placementdescriptionid_seq'::regclass)"))
    placementdescriptionname = Column(Text)


class Platform(Base):
    __tablename__ = 'platform'
    __table_args__ = {'schema': 'lqadb'}

    platformid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.platform_platformid_seq'::regclass)"))
    platformname = Column(Text)


class Region(Base):
    __tablename__ = 'region'
    __table_args__ = {'schema': 'lqadb'}

    regionid = Column(
        BigInteger, primary_key=True,
        server_default=text("nextval('lqadb.region_regionid_seq'::regclass)"))
    regionname = Column(Text)


class Retailer(Base):
    __tablename__ = 'retailer'
    __table_args__ = {'schema': 'lqadb'}

    retailerid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.retailer_retailerid_seq'::regclass)"))
    retailername = Column(Text)


class Serving(Base):
    __tablename__ = 'serving'
    __table_args__ = {'schema': 'lqadb'}

    servingid = Column(
        BigInteger, primary_key=True,
        server_default=text("nextval('lqadb.serving_servingid_seq'::regclass)"))
    servingname = Column(Text)


class Targetingbucket(Base):
    __tablename__ = 'targetingbucket'
    __table_args__ = {'schema': 'lqadb'}

    targetingbucketid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.targetingbucket_targetingbucketid_seq'::regclass)"))
    targetingbucketname = Column(Text)


class Transactionproductbroad(Base):
    __tablename__ = 'transactionproductbroad'
    __table_args__ = {'schema': 'lqadb'}

    transactionproductbroadid = Column(
        BigInteger, primary_key=True, server_default=text(
            "nextval('lqadb.transactionproductbroad_transactionproductbroadid_seq'::regclass)"))
    transactionproductbroadname = Column(Text)


class Transactionproductfine(Base):
    __tablename__ = 'transactionproductfine'
    __table_args__ = {'schema': 'lqadb'}

    transactionproductfineid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.transactionproductfine_transactionproductfineid_seq'::regclass)"))
    transactionproductfinename = Column(Text)


class Upload(Base):
    __tablename__ = 'upload'
    __table_args__ = {'schema': 'lqadb'}

    uploadid = Column(
        BigInteger, primary_key=True,
        server_default=text("nextval('lqadb.upload_uploadid_seq'::regclass)"))
    uploadname = Column(Text)
    datastartdate = Column(Date)
    dataenddate = Column(Date)
    lastuploaddate = Column(Date)


class Vendortype(Base):
    __tablename__ = 'vendortype'
    __table_args__ = {'schema': 'lqadb'}

    vendortypeid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.vendortype_vendortypeid_seq'::regclass)"))
    vendortypename = Column(Text)


class Age(Base):
    __tablename__ = 'age'
    __table_args__ = {'schema': 'lqadb'}

    ageid = Column(
        BigInteger, primary_key=True,
        server_default=text("nextval('lqadb.age_ageid_seq'::regclass)"))
    agename = Column(Text)
    demographicid = Column(ForeignKey(
        'lqadb.demographic.demographicid', ondelete='CASCADE'))

    demographic = relationship('Demographic')


class Client(Base):
    __tablename__ = 'client'
    __table_args__ = {'schema': 'lqadb'}

    clientid = Column(
        BigInteger, primary_key=True,
        server_default=text("nextval('lqadb.client_clientid_seq'::regclass)"))
    agencyid = Column(ForeignKey('lqadb.agency.agencyid', ondelete='CASCADE'))
    clientname = Column(Text)

    agency = relationship('Agency')


class Copy(Base):
    __tablename__ = 'copy'
    __table_args__ = {'schema': 'lqadb'}

    copyid = Column(
        BigInteger, primary_key=True,
        server_default=text("nextval('lqadb.copy_copyid_seq'::regclass)"))
    copyname = Column(Text)
    adid = Column(ForeignKey('lqadb.ad.adid', ondelete='CASCADE'))
    descriptionline1id = Column(ForeignKey(
        'lqadb.descriptionline1.descriptionline1id', ondelete='CASCADE'))
    descriptionline2id = Column(ForeignKey(
        'lqadb.descriptionline2.descriptionline2id', ondelete='CASCADE'))
    headline1id = Column(ForeignKey(
        'lqadb.headline1.headline1id', ondelete='CASCADE'))
    headline2id = Column(ForeignKey(
        'lqadb.headline2.headline2id', ondelete='CASCADE'))
    displayurlid = Column(ForeignKey(
        'lqadb.displayurl.displayurlid', ondelete='CASCADE'))
    uploadid = Column(ForeignKey('lqadb.upload.uploadid', ondelete='CASCADE'))

    ad = relationship('Ad')
    descriptionline1 = relationship('Descriptionline1')
    descriptionline2 = relationship('Descriptionline2')
    displayurl = relationship('Displayurl')
    headline1 = relationship('Headline1')
    headline2 = relationship('Headline2')
    upload = relationship('Upload')


class Country(Base):
    __tablename__ = 'country'
    __table_args__ = {'schema': 'lqadb'}

    countryid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.country_countryid_seq'::regclass)"))
    countryname = Column(Text)
    regionid = Column(ForeignKey('lqadb.region.regionid', ondelete='CASCADE'))

    region = relationship('Region')


class Creative(Base):
    __tablename__ = 'creative'
    __table_args__ = {'schema': 'lqadb'}

    creativeid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.creative_creativeid_seq'::regclass)"))
    creativename = Column(Text)
    adsizeid = Column(ForeignKey('lqadb.adsize.adsizeid', ondelete='CASCADE'))
    adformatid = Column(ForeignKey(
        'lqadb.adformat.adformatid', ondelete='CASCADE'))
    adtypeid = Column(ForeignKey('lqadb.adtype.adtypeid', ondelete='CASCADE'))
    ctaid = Column(ForeignKey('lqadb.cta.ctaid', ondelete='CASCADE'))
    creativedescriptionid = Column(ForeignKey(
        'lqadb.creativedescription.creativedescriptionid', ondelete='CASCADE'))
    characterid = Column(ForeignKey(
        'lqadb.character.characterid', ondelete='CASCADE'))
    creativemodifierid = Column(ForeignKey(
        'lqadb.creativemodifier.creativemodifierid', ondelete='CASCADE'))
    creativelineitemid = Column(ForeignKey(
        'lqadb.creativelineitem.creativelineitemid', ondelete='CASCADE'))
    creativelengthid = Column(ForeignKey(
        'lqadb.creativelength.creativelengthid', ondelete='CASCADE'))
    uploadid = Column(ForeignKey('lqadb.upload.uploadid', ondelete='CASCADE'))

    adformat = relationship('Adformat')
    adsize = relationship('Adsize')
    adtype = relationship('Adtype')
    character = relationship('Character')
    creativedescription = relationship('Creativedescription')
    creativelength = relationship('Creativelength')
    creativelineitem = relationship('Creativelineitem')
    creativemodifier = relationship('Creativemodifier')
    cta = relationship('Cta')
    upload = relationship('Upload')


class Transactionproduct(Base):
    __tablename__ = 'transactionproduct'
    __table_args__ = {'schema': 'lqadb'}

    transactionproductid = Column(
        BigInteger, primary_key=True,
        server_default=text(
            "nextval('lqadb.transactionproduct_transactionproductid_seq'::regclass)"))
    transactionproductname = Column(Text)
    transactionproductbroadid = Column(ForeignKey(
        'lqadb.transactionproductbroad.transactionproductbroadid',
        ondelete='CASCADE'))
    transactionproductfineid = Column(ForeignKey(
        'lqadb.transactionproductfine.transactionproductfineid',
        ondelete='CASCADE'))

    transactionproductbroad = relationship('Transactionproductbroad')
    transactionproductfine = relationship('Transactionproductfine')


class Vendor(Base):
    __tablename__ = 'vendor'
    __table_args__ = {'schema': 'lqadb'}

    vendorid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.vendor_vendorid_seq'::regclass)"))
    vendorname = Column(Text)
    vendortypeid = Column(ForeignKey(
        'lqadb.vendortype.vendortypeid', ondelete='CASCADE'))

    vendortype = relationship('Vendortype')


class Product(Base):
    __tablename__ = 'product'
    __table_args__ = {'schema': 'lqadb'}

    productid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.product_productid_seq'::regclass)"))
    productname = Column(Text)
    productdetail = Column(Text)
    clientid = Column(ForeignKey('lqadb.client.clientid', ondelete='CASCADE'))

    client = relationship('Client')


class Targeting(Base):
    __tablename__ = 'targeting'
    __table_args__ = {'schema': 'lqadb'}

    targetingid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.targeting_targetingid_seq'::regclass)"))
    targetingname = Column(Text)
    ageid = Column(ForeignKey('lqadb.age.ageid', ondelete='CASCADE'))
    genderid = Column(ForeignKey('lqadb.gender.genderid', ondelete='CASCADE'))
    datatype1id = Column(ForeignKey(
        'lqadb.datatype1.datatype1id', ondelete='CASCADE'))
    datatype2id = Column(ForeignKey(
        'lqadb.datatype2.datatype2id', ondelete='CASCADE'))
    targetingbucketid = Column(ForeignKey(
        'lqadb.targetingbucket.targetingbucketid', ondelete='CASCADE'))
    genretargetingid = Column(ForeignKey(
        'lqadb.genretargeting.genretargetingid', ondelete='CASCADE'))
    genretargetingfineid = Column(ForeignKey(
        'lqadb.genretargetingfine.genretargetingfineid', ondelete='CASCADE'))
    uploadid = Column(ForeignKey('lqadb.upload.uploadid', ondelete='CASCADE'))

    age = relationship('Age')
    datatype1 = relationship('Datatype1')
    datatype2 = relationship('Datatype2')
    gender = relationship('Gender')
    genretargetingfine = relationship('Genretargetingfine')
    genretargeting = relationship('Genretargeting')
    targetingbucket = relationship('Targetingbucket')
    upload = relationship('Upload')


class Campaign(Base):
    __tablename__ = 'campaign'
    __table_args__ = {'schema': 'lqadb'}

    campaignid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.campaign_campaignid_seq'::regclass)"))
    campaignname = Column(Text)
    campaigntype = Column(Text)
    campaignphase = Column(Text)
    campaigntiming = Column(Text)
    productid = Column(ForeignKey(
        'lqadb.product.productid', ondelete='CASCADE'))
    uploadid = Column(ForeignKey('lqadb.upload.uploadid', ondelete='CASCADE'))

    product = relationship('Product')
    upload = relationship('Upload')


class Fullplacement(Base):
    __tablename__ = 'fullplacement'
    __table_args__ = {'schema': 'lqadb'}

    fullplacementid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.fullplacement_fullplacementid_seq'::regclass)"))
    fullplacementname = Column(Text)
    campaignid = Column(ForeignKey(
        'lqadb.campaign.campaignid', ondelete='CASCADE'), index=True)
    vendorid = Column(ForeignKey('lqadb.vendor.vendorid', ondelete='CASCADE'))
    countryid = Column(ForeignKey(
        'lqadb.country.countryid', ondelete='CASCADE'))
    mediachannelid = Column(ForeignKey(
        'lqadb.mediachannel.mediachannelid', ondelete='CASCADE'))
    targetingid = Column(ForeignKey(
        'lqadb.targeting.targetingid', ondelete='CASCADE'), index=True)
    creativeid = Column(ForeignKey(
        'lqadb.creative.creativeid', ondelete='CASCADE'), index=True)
    copyid = Column(ForeignKey(
        'lqadb.copy.copyid', ondelete='CASCADE'), index=True)
    buymodelid = Column(ForeignKey(
        'lqadb.buymodel.buymodelid', ondelete='CASCADE'))
    buyrate = Column(Text)
    placementdate = Column(Date)
    startdate = Column(Date)
    enddate = Column(Date)
    servingid = Column(ForeignKey(
        'lqadb.serving.servingid', ondelete='CASCADE'))
    retailerid = Column(ForeignKey(
        'lqadb.retailer.retailerid', ondelete='CASCADE'))
    environmentid = Column(ForeignKey(
        'lqadb.environment.environmentid', ondelete='CASCADE'))
    kpiid = Column(ForeignKey('lqadb.kpi.kpiid', ondelete='CASCADE'))
    uploadid = Column(ForeignKey('lqadb.upload.uploadid', ondelete='CASCADE'))
    modelid = Column(ForeignKey('lqadb.model.modelid', ondelete='CASCADE'))
    factionid = Column(ForeignKey(
        'lqadb.faction.factionid', ondelete='CASCADE'))
    platformid = Column(ForeignKey(
        'lqadb.platform.platformid', ondelete='CASCADE'))
    transactionproductid = Column(ForeignKey(
        'lqadb.transactionproduct.transactionproductid', ondelete='CASCADE'))
    placementid = Column(ForeignKey(
        'lqadb.placement.placementid', ondelete='CASCADE'))
    placementdescriptionid = Column(ForeignKey(
        'lqadb.placementdescription.placementdescriptionid',
        ondelete='CASCADE'))
    packagedescriptionid = Column(ForeignKey(
        'lqadb.packagedescription.packagedescriptionid', ondelete='CASCADE'))

    buymodel = relationship('Buymodel')
    campaign = relationship('Campaign')
    copy = relationship('Copy')
    country = relationship('Country')
    creative = relationship('Creative')
    environment = relationship('Environment')
    faction = relationship('Faction')
    kpi = relationship('Kpi')
    mediachannel = relationship('Mediachannel')
    model = relationship('Model')
    packagedescription = relationship('Packagedescription')
    placementdescription = relationship('Placementdescription')
    placement = relationship('Placement')
    platform = relationship('Platform')
    retailer = relationship('Retailer')
    serving = relationship('Serving')
    targeting = relationship('Targeting')
    transactionproduct = relationship('Transactionproduct')
    upload = relationship('Upload')
    vendor = relationship('Vendor')


class Event(Base):
    __tablename__ = 'event'
    __table_args__ = {'schema': 'lqadb'}

    eventid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.event_eventid_seq'::regclass)"))
    eventname = Column(Text)
    eventdate = Column(Date)
    fullplacementid = Column(ForeignKey(
        'lqadb.fullplacement.fullplacementid', ondelete='CASCADE'), index=True)
    impressions = Column(Numeric)
    clicks = Column(Numeric)
    netcost = Column(Numeric)
    adservingcost = Column(Numeric)
    agencyfees = Column(Numeric)
    totalcost = Column(Numeric)
    videoviews = Column(Numeric)
    videoviews25 = Column(Numeric)
    videoviews50 = Column(Numeric)
    videoviews75 = Column(Numeric)
    videoviews100 = Column(Numeric)
    landingpage = Column(Numeric)
    homepage = Column(Numeric)
    buttonclick = Column(Numeric)
    purchase = Column(Numeric)
    signup = Column(Numeric)
    gameplayed = Column(Numeric)
    gameplayed3 = Column(Numeric)
    gameplayed6 = Column(Numeric)
    landingpage_pi = Column(Numeric)
    landingpage_pc = Column(Numeric)
    homepage_pi = Column(Numeric)
    homepage_pc = Column(Numeric)
    buttonclick_pi = Column(Numeric)
    buttonclick_pc = Column(Numeric)
    purchase_pi = Column(Numeric)
    purchase_pc = Column(Numeric)
    signup_pi = Column(Numeric)
    signup_pc = Column(Numeric)
    gameplayed_pi = Column(Numeric)
    gameplayed_pc = Column(Numeric)
    gameplayed3_pi = Column(Numeric)
    gameplayed3_pc = Column(Numeric)
    gameplayed6_pi = Column(Numeric)
    gameplayed6_pc = Column(Numeric)
    uploadid = Column(ForeignKey('lqadb.upload.uploadid', ondelete='CASCADE'))
    reach = Column(Numeric)
    frequency = Column(Numeric)
    engagements = Column(Numeric)
    likes = Column(Numeric)
    revenue = Column(Numeric)
    newuser = Column(Numeric)
    activeuser = Column(Numeric)
    download = Column(Numeric)
    login = Column(Numeric)
    newuser_pi = Column(Numeric)
    activeuser_pi = Column(Numeric)
    download_pi = Column(Numeric)
    login_pi = Column(Numeric)
    newuser_pc = Column(Numeric)
    activeuser_pc = Column(Numeric)
    download_pc = Column(Numeric)
    login_pc = Column(Numeric)
    retention_day1 = Column(Numeric)
    retention_day3 = Column(Numeric)
    retention_day7 = Column(Numeric)
    retention_day14 = Column(Numeric)
    retention_day30 = Column(Numeric)
    retention_day60 = Column(Numeric)
    retention_day90 = Column(Numeric)
    retention_day120 = Column(Numeric)
    total_user = Column(Numeric)
    paying_user = Column(Numeric)
    transaction = Column(Numeric)
    match_played = Column(Numeric)
    sm_totalbuzz = Column(Numeric)
    sm_totalbuzzpost = Column(Numeric)
    sm_totalreplies = Column(Numeric)
    sm_totalreposts = Column(Numeric)
    sm_originalposts = Column(Numeric)
    sm_impressions = Column(Numeric)
    sm_positivesentiment = Column(Numeric)
    sm_negativesentiment = Column(Numeric)
    sm_passion = Column(Numeric)
    sm_uniqueauthors = Column(Numeric)
    sm_strongemotion = Column(Numeric)
    sm_weakemotion = Column(Numeric)
    transaction_revenue = Column(Numeric)
    revenue_userstart = Column(Numeric)
    revenue_userstart_30day = Column(Numeric)
    reportingcost = Column(Numeric)
    trueviewviews = Column(Numeric)
    fb3views = Column(Numeric)
    fb10views = Column(Numeric)
    dcmservicefee = Column(Numeric)
    view_imps = Column(Numeric)
    view_tot_imps = Column(Numeric)
    view_fraud = Column(Numeric)
    ga_sessions = Column(Numeric)
    ga_goal1 = Column(Numeric)
    ga_goal2 = Column(Numeric)
    ga_pageviews = Column(Numeric)
    ga_bounces = Column(Numeric)
    comments = Column(Numeric)
    shares = Column(Numeric)
    reactions = Column(Numeric)
    checkout = Column(Numeric)
    checkoutpi = Column(Numeric)
    checkoutpc = Column(Numeric)
    reach_campaign = Column('reach-campaign', Numeric)
    reach_date = Column('reach-date', Numeric)
    reach_campaign1 = Column('reach_campaign', Numeric)
    reach_date1 = Column('reach_date', Numeric)
    ga_timeonpage = Column(Numeric)
    signup_ss = Column(Numeric)
    landingpage_ss = Column(Numeric)
    view_monitored_imps = Column(Numeric)
    verificationcost = Column(Numeric)
    videoplays = Column(Numeric)
    ad_recallers = Column(Numeric)

    fullplacement = relationship('Fullplacement')
    upload = relationship('Upload')


class Plan(Base):
    __tablename__ = 'plan'
    __table_args__ = {'schema': 'lqadb'}

    planid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.plan_planid_seq'::regclass)"))
    planname = Column(Text)
    eventdate = Column(Date)
    fullplacementid = Column(ForeignKey(
        'lqadb.fullplacement.fullplacementid', ondelete='CASCADE'))
    plannednetcost = Column(Numeric)
    uploadid = Column(ForeignKey('lqadb.upload.uploadid', ondelete='CASCADE'))

    fullplacement = relationship('Fullplacement')
    upload = relationship('Upload')


class EventSteam(Base):
    __tablename__ = 'eventsteam'
    __table_args__ = {'schema': 'lqadb'}

    eventsteamid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.eventsteam_eventsteamid_seq'::regclass)"))
    eventsteamname = Column(Text)
    eventid = Column(ForeignKey(
        'lqadb.event.eventid', ondelete='CASCADE'))
    steam_totalvisits = Column(Numeric)
    steam_trackedvisits = Column(Numeric)
    steam_wishlists = Column(Numeric)
    steam_purchases = Column(Numeric)
    steam_activations = Column(Numeric)

    uploadid = Column(ForeignKey('lqadb.upload.uploadid', ondelete='CASCADE'))

    event = relationship('Event')
    upload = relationship('Upload')


class EventConv(Base):
    __tablename__ = 'eventconv'
    __table_args__ = {'schema': 'lqadb'}

    eventconvid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.eventconv_eventconvid_seq'::regclass)"))
    eventconvname = Column(Text)
    eventid = Column(ForeignKey(
        'lqadb.event.eventid', ondelete='CASCADE'))
    conv1_cpa = Column(Numeric)
    conv2 = Column(Numeric)
    conv3 = Column(Numeric)
    conv4 = Column(Numeric)
    conv5 = Column(Numeric)
    conv6 = Column(Numeric)
    conv7 = Column(Numeric)
    conv8 = Column(Numeric)
    conv9 = Column(Numeric)
    conv10 = Column(Numeric)

    uploadid = Column(ForeignKey('lqadb.upload.uploadid', ondelete='CASCADE'))

    event = relationship('Event')
    upload = relationship('Upload')


class EventPlan(Base):
    __tablename__ = 'eventplan'
    __table_args__ = {'schema': 'lqadb'}

    eventplanid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.eventplan_eventplanid_seq'::regclass)"))
    eventplanname = Column(Text)
    eventid = Column(ForeignKey(
        'lqadb.event.eventid', ondelete='CASCADE'))
    plan_impressions = Column(Numeric)
    plan_clicks = Column(Numeric)
    plan_netcost = Column(Numeric)
    plan_adservingcost = Column(Numeric)
    plan_agencyfees = Column(Numeric)
    plan_totalcost = Column(Numeric)
    plan_videoviews = Column(Numeric)
    plan_videoviews25 = Column(Numeric)
    plan_videoviews50 = Column(Numeric)
    plan_videoviews75 = Column(Numeric)
    plan_videoviews100 = Column(Numeric)
    plan_landingpage = Column(Numeric)
    plan_homepage = Column(Numeric)
    plan_buttonclick = Column(Numeric)
    plan_purchase = Column(Numeric)
    plan_signup = Column(Numeric)
    plan_verificationcost = Column(Numeric)
    plan_reportingcost = Column(Numeric)
    plan_dcmservicefee = Column(Numeric)

    uploadid = Column(ForeignKey('lqadb.upload.uploadid', ondelete='CASCADE'))

    event = relationship('Event')
    upload = relationship('Upload')


class EventBrand(Base):
    __tablename__ = 'eventbrand'
    __table_args__ = {'schema': 'lqadb'}

    eventbrandid = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('lqadb.eventbrand_eventbrandid_seq'::regclass)"))
    eventbrandname = Column(Text)
    eventid = Column(ForeignKey(
        'lqadb.event.eventid', ondelete='CASCADE'))
    media_spend = Column(Numeric)
    youtube_subscribers = Column(Numeric)
    twitter_followers = Column(Numeric)
    twitch_views = Column(Numeric)
    twitch_viewers = Column(Numeric)
    subreddit_members = Column(Numeric)
    player_share = Column(Numeric)
    nz_awareness = Column(Numeric)
    np_score = Column(Numeric)
    coverage = Column(Numeric)
    month_avg_user = Column(Numeric)
    stickiness = Column(Numeric)
    days_played = Column(Numeric)
    play_intent = Column(Numeric)

    uploadid = Column(ForeignKey('lqadb.upload.uploadid', ondelete='CASCADE'))

    event = relationship('Event')
    upload = relationship('Upload')
