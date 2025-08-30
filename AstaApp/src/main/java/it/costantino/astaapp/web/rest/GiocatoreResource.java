package it.costantino.astaapp.web.rest;

import it.costantino.astaapp.repository.GiocatoreRepository;
import it.costantino.astaapp.service.GiocatoreService;
import it.costantino.astaapp.service.dto.GiocatoreDTO;
import it.costantino.astaapp.web.rest.errors.BadRequestAlertException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tech.jhipster.web.util.HeaderUtil;
import tech.jhipster.web.util.ResponseUtil;

/**
 * REST controller for managing {@link it.costantino.astaapp.domain.Giocatore}.
 */
@RestController
@RequestMapping("/api/giocatores")
public class GiocatoreResource {

    private static final Logger LOG = LoggerFactory.getLogger(GiocatoreResource.class);

    private static final String ENTITY_NAME = "giocatore";

    @Value("${jhipster.clientApp.name}")
    private String applicationName;

    private final GiocatoreService giocatoreService;

    private final GiocatoreRepository giocatoreRepository;

    public GiocatoreResource(GiocatoreService giocatoreService, GiocatoreRepository giocatoreRepository) {
        this.giocatoreService = giocatoreService;
        this.giocatoreRepository = giocatoreRepository;
    }

    /**
     * {@code POST  /giocatores} : Create a new giocatore.
     *
     * @param giocatoreDTO the giocatoreDTO to create.
     * @return the {@link ResponseEntity} with status {@code 201 (Created)} and with body the new giocatoreDTO, or with status {@code 400 (Bad Request)} if the giocatore has already an ID.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PostMapping("")
    public ResponseEntity<GiocatoreDTO> createGiocatore(@RequestBody GiocatoreDTO giocatoreDTO) throws URISyntaxException {
        LOG.debug("REST request to save Giocatore : {}", giocatoreDTO);
        if (giocatoreDTO.getId() != null) {
            throw new BadRequestAlertException("A new giocatore cannot already have an ID", ENTITY_NAME, "idexists");
        }
        giocatoreDTO = giocatoreService.save(giocatoreDTO);
        return ResponseEntity.created(new URI("/api/giocatores/" + giocatoreDTO.getId()))
            .headers(HeaderUtil.createEntityCreationAlert(applicationName, false, ENTITY_NAME, giocatoreDTO.getId().toString()))
            .body(giocatoreDTO);
    }

    /**
     * {@code PUT  /giocatores/:id} : Updates an existing giocatore.
     *
     * @param id the id of the giocatoreDTO to save.
     * @param giocatoreDTO the giocatoreDTO to update.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the updated giocatoreDTO,
     * or with status {@code 400 (Bad Request)} if the giocatoreDTO is not valid,
     * or with status {@code 500 (Internal Server Error)} if the giocatoreDTO couldn't be updated.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PutMapping("/{id}")
    public ResponseEntity<GiocatoreDTO> updateGiocatore(
        @PathVariable(value = "id", required = false) final Long id,
        @RequestBody GiocatoreDTO giocatoreDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to update Giocatore : {}, {}", id, giocatoreDTO);
        if (giocatoreDTO.getId() == null) {
            throw new BadRequestAlertException("Invalid id", ENTITY_NAME, "idnull");
        }
        if (!Objects.equals(id, giocatoreDTO.getId())) {
            throw new BadRequestAlertException("Invalid ID", ENTITY_NAME, "idinvalid");
        }

        if (!giocatoreRepository.existsById(id)) {
            throw new BadRequestAlertException("Entity not found", ENTITY_NAME, "idnotfound");
        }

        giocatoreDTO = giocatoreService.update(giocatoreDTO);
        return ResponseEntity.ok()
            .headers(HeaderUtil.createEntityUpdateAlert(applicationName, false, ENTITY_NAME, giocatoreDTO.getId().toString()))
            .body(giocatoreDTO);
    }

    /**
     * {@code PATCH  /giocatores/:id} : Partial updates given fields of an existing giocatore, field will ignore if it is null
     *
     * @param id the id of the giocatoreDTO to save.
     * @param giocatoreDTO the giocatoreDTO to update.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the updated giocatoreDTO,
     * or with status {@code 400 (Bad Request)} if the giocatoreDTO is not valid,
     * or with status {@code 404 (Not Found)} if the giocatoreDTO is not found,
     * or with status {@code 500 (Internal Server Error)} if the giocatoreDTO couldn't be updated.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PatchMapping(value = "/{id}", consumes = { "application/json", "application/merge-patch+json" })
    public ResponseEntity<GiocatoreDTO> partialUpdateGiocatore(
        @PathVariable(value = "id", required = false) final Long id,
        @RequestBody GiocatoreDTO giocatoreDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to partial update Giocatore partially : {}, {}", id, giocatoreDTO);
        if (giocatoreDTO.getId() == null) {
            throw new BadRequestAlertException("Invalid id", ENTITY_NAME, "idnull");
        }
        if (!Objects.equals(id, giocatoreDTO.getId())) {
            throw new BadRequestAlertException("Invalid ID", ENTITY_NAME, "idinvalid");
        }

        if (!giocatoreRepository.existsById(id)) {
            throw new BadRequestAlertException("Entity not found", ENTITY_NAME, "idnotfound");
        }

        Optional<GiocatoreDTO> result = giocatoreService.partialUpdate(giocatoreDTO);

        return ResponseUtil.wrapOrNotFound(
            result,
            HeaderUtil.createEntityUpdateAlert(applicationName, false, ENTITY_NAME, giocatoreDTO.getId().toString())
        );
    }

    /**
     * {@code GET  /giocatores} : get all the giocatores.
     *
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and the list of giocatores in body.
     */
    @GetMapping("")
    public List<GiocatoreDTO> getAllGiocatores() {
        LOG.debug("REST request to get all Giocatores");
        return giocatoreService.findAll();
    }

    /**
     * {@code GET  /giocatores/:id} : get the "id" giocatore.
     *
     * @param id the id of the giocatoreDTO to retrieve.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the giocatoreDTO, or with status {@code 404 (Not Found)}.
     */
    @GetMapping("/{id}")
    public ResponseEntity<GiocatoreDTO> getGiocatore(@PathVariable("id") Long id) {
        LOG.debug("REST request to get Giocatore : {}", id);
        Optional<GiocatoreDTO> giocatoreDTO = giocatoreService.findOne(id);
        return ResponseUtil.wrapOrNotFound(giocatoreDTO);
    }

    /**
     * {@code DELETE  /giocatores/:id} : delete the "id" giocatore.
     *
     * @param id the id of the giocatoreDTO to delete.
     * @return the {@link ResponseEntity} with status {@code 204 (NO_CONTENT)}.
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteGiocatore(@PathVariable("id") Long id) {
        LOG.debug("REST request to delete Giocatore : {}", id);
        giocatoreService.delete(id);
        return ResponseEntity.noContent()
            .headers(HeaderUtil.createEntityDeletionAlert(applicationName, false, ENTITY_NAME, id.toString()))
            .build();
    }
}
