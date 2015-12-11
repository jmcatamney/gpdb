//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerLogicalCTEProducer.h
//
//	@doc:
//		Parse handler for parsing a logical CTE producer operator
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerLogicalCTEProducer_H
#define GPDXL_CParseHandlerLogicalCTEProducer_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerLogicalCTEProducer
	//
	//	@doc:
	//		Parse handler for parsing a logical CTE producer operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerLogicalCTEProducer : public CParseHandlerLogicalOp
	{
		private:

			// private copy ctor
			CParseHandlerLogicalCTEProducer(const CParseHandlerLogicalCTEProducer &);

			// process the start of an element
			virtual
			void StartElement
					(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
 					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname,		// element's qname
					const Attributes& attr				// element's attributes
					);

			// process the end of an element
			virtual
			void EndElement
					(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname		// element's qname
					);

		public:
			// ctor
			CParseHandlerLogicalCTEProducer
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerLogicalCTEProducer_H

// EOF
