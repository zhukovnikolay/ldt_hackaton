CREATE TABLE "public.customs_customtnvedcode" (
	"id" serial NOT NULL,
	"tnved_code" varchar(255) NOT NULL UNIQUE,
	"tnved_name" varchar(255) NOT NULL UNIQUE,
	"created_at" TIMESTAMP,
	"updated_at" TIMESTAMP,
	"tnved_id" int NOT NULL,
	CONSTRAINT "customs_customtnvedcode_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.customs_customdata" (
	"id" serial NOT NULL,
	"tnved_id" int NOT NULL,
	"direction" varchar(1) NOT NULL,
	"country_id" int NOT NULL,
	"period" DATE NOT NULL,
	"region_id" int NOT NULL,
	"unit_id" int NOT NULL,
	"price" DECIMAL NOT NULL,
	"volume" DECIMAL NOT NULL,
	"quantity" DECIMAL NOT NULL,
	"created_at" timestamp with time zone,
	"updated_at" timestamp with time zone,
	CONSTRAINT "customs_customdata_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.customs_country" (
	"id" serial NOT NULL,
	"country_name" varchar(255) NOT NULL,
	"country_block" varchar(255),
	"created_at" timestamp with time zone,
	"updated_at" timestamp with time zone,
	"country_id" int NOT NULL UNIQUE,
	CONSTRAINT "customs_country_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.customs_unit" (
	"id" serial NOT NULL,
	"unit_code" varchar(255) NOT NULL,
	"unit_name" varchar(255) NOT NULL,
	"created_at" timestamp with time zone,
	"updated_at" timestamp with time zone,
	"unit_id" int NOT NULL,
	CONSTRAINT "customs_unit_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.customs_sanction" (
	"id" serial NOT NULL,
	"tnved_id" int NOT NULL,
	"country_id" int NOT NULL,
	"direction" varchar(1) NOT NULL,
	CONSTRAINT "customs_sanction_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.customs_region" (
	"id" serial NOT NULL,
	"region_code" varchar(255) NOT NULL,
	"region_name" varchar(255) NOT NULL,
	"federal_district_id" int NOT NULL,
	"created_at" timestamp with time zone,
	"updated_at" timestamp with time zone,
	"region_id" int NOT NULL UNIQUE,
	CONSTRAINT "customs_region_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.customs_federaldistrict" (
	"id" serial NOT NULL,
	"federal_district_code" varchar(255) NOT NULL,
	"federal_district_name" varchar(255) NOT NULL,
	"created_at" timestamp with time zone,
	"updated_at" timestamp with time zone,
	"federal_district_id" int NOT NULL UNIQUE,
	CONSTRAINT "customs_federaldistrict_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE "public.customs_recommendation" (
	"id" serial NOT NULL,
	"tnved_id" int NOT NULL,
	"region_id" smallint NOT NULL,
	CONSTRAINT "customs_recommendation_pk" PRIMARY KEY ("id")
) WITH (
  OIDS=FALSE
);




ALTER TABLE "customs_customdata" ADD CONSTRAINT "customs_customdata_fk0" FOREIGN KEY ("tnved_id") REFERENCES "customs_customtnvedcode"("tnved_id");
ALTER TABLE "customs_customdata" ADD CONSTRAINT "customs_customdata_fk1" FOREIGN KEY ("country_id") REFERENCES "customs_country"("country_id");
ALTER TABLE "customs_customdata" ADD CONSTRAINT "customs_customdata_fk2" FOREIGN KEY ("region_id") REFERENCES "customs_region"("region_id");
ALTER TABLE "customs_customdata" ADD CONSTRAINT "customs_customdata_fk3" FOREIGN KEY ("unit_id") REFERENCES "customs_unit"("unit_id");



ALTER TABLE "customs_sanction" ADD CONSTRAINT "customs_sanction_fk0" FOREIGN KEY ("tnved_id") REFERENCES "customs_customtnvedcode"("tnved_id");
ALTER TABLE "customs_sanction" ADD CONSTRAINT "customs_sanction_fk1" FOREIGN KEY ("country_id") REFERENCES "customs_country"("country_id");

ALTER TABLE "customs_region" ADD CONSTRAINT "customs_region_fk0" FOREIGN KEY ("federal_district_id") REFERENCES "customs_federaldistrict"("federal_district_id");


ALTER TABLE "customs_recommendation" ADD CONSTRAINT "customs_recommendation_fk0" FOREIGN KEY ("tnved_id") REFERENCES "customs_customtnvedcode"("tnved_id");
ALTER TABLE "customs_recommendation" ADD CONSTRAINT "customs_recommendation_fk1" FOREIGN KEY ("region_id") REFERENCES "customs_region"("region_id");









